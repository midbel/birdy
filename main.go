package main

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

func init() {
	sql.Register("dry", getDriver())
}

func main() {
	args, err := parseArgs()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	list, err := extractSQL(args.File, args.Spec)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	switch cmd := flag.Arg(0); cmd {
	case "run":
		err = args.dsn.Exec(list)
	case "info":
		for _, m := range list {
			n, _ := fmt.Fprintf(os.Stdout, "migration #%d", m.Group)
			fmt.Fprintln(os.Stdout)
			fmt.Fprintln(os.Stdout, strings.Repeat("=", n))
			fmt.Fprintf(os.Stdout, "- %d groups", m.Block())
			fmt.Fprintln(os.Stdout)
			fmt.Fprintf(os.Stdout, "- %d queries", m.Count())
			fmt.Fprintln(os.Stdout)
		}
	case "history":
	default:
		err = fmt.Errorf("%s: unknown command", cmd)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
}

type Args struct {
	dsn  dsnInfo
	File string
	Spec string
}

func parseArgs() (Args, error) {
	var (
		dsn dsnInfo
		arg Args
	)
	flag.StringVar(&dsn.User, "u", os.Getenv("username"), "database username")
	flag.StringVar(&dsn.Pass, "w", os.Getenv("password"), "database user password")
	flag.StringVar(&dsn.Host, "s", os.Getenv("server"), "server name")
	flag.IntVar(&dsn.Port, "p", 0, "server port")
	flag.StringVar(&dsn.Name, "d", os.Getenv("database"), "database name")
	flag.StringVar(&dsn.Driver, "i", os.Getenv("driver"), "database driver")
	flag.Parse()

	if flag.NArg() == 2 {
		arg.File = flag.Arg(1)
		arg.Spec = "1"
	} else if flag.NArg() == 3 {
		arg.File = flag.Arg(2)
		arg.Spec = flag.Arg(1)
	} else {
		return arg, fmt.Errorf("invalid number of arguments given")
	}
	arg.dsn = dsn
	return arg, nil
}

func extractSQL(file, spec string) ([]Migration, error) {
	s := createSplitter()
	return s.Load(file, spec)
}

type TxMode int

const (
	TxDefault TxMode = iota
	TxStmt
	TxOff
)

func getTransactionMode(str string) TxMode {
	switch str {
	case "off":
		return TxOff
	case "statement", "stmt", "query":
		return TxStmt
	default:
		return TxDefault
	}
}

type ErrMode int

const (
	ErrDefault ErrMode = iota
	ErrSilent
	ErrWarning
)

func getErrorMode(str string) ErrMode {
	switch str {
	case "ignore", "silent":
		return ErrSilent
	case "warning":
		return ErrWarning
	default:
		return ErrDefault
	}
}

type Unit struct {
	Query string
	Group int
	Error ErrMode
}

func (u Unit) RollbackOnError() bool {
	return u.Error == ErrDefault
}

type Stack []Migration

func (s *Stack) New(m Migration) {
	if len(*s) == 1 && len((*s)[0].Queries) == 0 {
		(*s)[0] = m
		return
	}
	*s = append(*s, m)
}

func (s *Stack) Push(u Unit) {
	x := len(*s) - 1
	m := (*s)[x]
	m.Queries = append(m.Queries, u)
	(*s)[x] = m
}

type Migration struct {
	File    string
	Queries []Unit
	Group   int
	errMode ErrMode
	txMode  TxMode
}

func (m Migration) Block() int {
	seen := make(map[int]struct{})
	for i := range m.Queries {
		seen[m.Queries[i].Group] = struct{}{}
	}
	return len(seen)
}

func (m Migration) Count() int {
	return len(m.Queries)
}

const (
	semi = byte(';')
	nl   = byte('\n')
)

type splitter struct {
	delimiter []byte
	group     int
	up        bool
	ignore    bool

	errMode ErrMode
	txMode  TxMode
}

func createSplitter() *splitter {
	return &splitter{
		delimiter: []byte{semi},
		up:        true,
	}
}

func (s *splitter) Load(file, spec string) ([]Migration, error) {
	files, err := os.ReadDir(file)
	if err != nil {
		return s.load(file)
	}
	if files, err = getFilesFromSpec(spec, files); err != nil {
		return nil, err
	}

	var all []Migration
	for _, e := range files {
		origin := filepath.Join(file, e.Name())
		units, err := s.load(origin)
		if err != nil {
			return nil, err
		}
		all = append(all, units...)
	}
	return all, nil
}

func (s *splitter) load(file string) (Stack, error) {
	r, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	s.reset()
	return s.Split(r)
}

func (s *splitter) Split(r io.Reader) (Stack, error) {
	var (
		scan = bufio.NewScanner(r)
		list Stack
	)
	list.New(s.getMigration())
	scan.Split(s.splitStmt)
	for scan.Scan() {
		sql := scan.Text()
		if !s.isRegularDelimiter() {
			sql = strings.TrimRight(sql, string(s.delimiter))
			sql = strings.TrimSpace(sql)
		}
		if s.updateState(sql) {
			list.New(s.getMigration())
			continue
		}
		if len(sql) == 0 || s.ignore {
			continue
		}
		list.Push(s.getUnit(sql))
	}
	return list, scan.Err()
}

func (s *splitter) getMigration() Migration {
	return Migration{
		File:    "",
		Group:   s.group,
		errMode: s.errMode,
		txMode:  s.txMode,
	}
}

func (s *splitter) getUnit(sql string) Unit {
	return Unit{
		Query: sql,
		Group: s.group,
	}
}

func (s *splitter) updateState(sql string) bool {
	s.revertDelimiter()
	if !strings.HasPrefix(sql, "--") {
		return false
	}
	sql = strings.TrimPrefix(sql, "--")
	sql = strings.TrimSpace(sql)
	block, options, _ := strings.Cut(sql, ";")
	switch block = strings.TrimSpace(block); {
	case block == "migrate" || block == "":
		s.group++
		s.ignore = false
		s.resetOptions(options)
	case block == "ignore":
		s.ignore = true
	case strings.HasPrefix(sql, "delimiter"):
		sql = strings.TrimPrefix(sql, "delimiter")
		sql = strings.TrimSpace(sql)
		s.delimiter = []byte(sql)
	default:
		return false
	}
	return true
}

func (s *splitter) resetOptions(options string) error {
	parts := strings.Split(options, ",")
	for _, str := range parts {
		key, value, _ := strings.Cut(str, "=")
		switch v := strings.TrimSpace(value); strings.TrimSpace(key) {
		case "error":
			s.errMode = getErrorMode(v)
		case "transaction":
			s.txMode = getTransactionMode(v)
		default:
		}
	}
	return nil
}

func (s *splitter) reset() {
	s.revertDelimiter()
	s.up = true
	s.group++
}

func (s *splitter) revertDelimiter() {
	s.delimiter = []byte{semi}
}

func (s *splitter) isRegularDelimiter() bool {
	return bytes.Equal(s.delimiter, []byte{semi})
}

func (s *splitter) splitStmt(data []byte, atEof bool) (int, []byte, error) {
	var offset int
	if s.isRegularDelimiter() {
		offset = len(data)
		data = bytes.TrimLeft(data, "\x0a\x20")
		offset -= len(data)
		if bytes.HasPrefix(data, []byte("--")) {
			ix := bytes.IndexByte(data, nl)
			ix++
			return offset + ix, bytes.TrimSpace(data[:ix]), nil
		}
	}
	ix := bytes.Index(data, s.delimiter)
	if ix < 0 && atEof {
		if len(data) == 0 {
			return 0, nil, bufio.ErrFinalToken
		}
		return 0, nil, fmt.Errorf("incomplete statement at end of file")
	}
	if ix < 0 && !atEof {
		return 0, nil, nil
	}
	ix += len(s.delimiter)
	return offset + ix, bytes.TrimSpace(data[:ix]), nil
}

type Range struct {
	Start int
	End   int
}

func (r Range) peek(list []os.DirEntry) ([]os.DirEntry, error) {
	if r.isFull() {
		return list, nil
	}
	if r.End == 0 && r.Start != 0 {
		r.End = len(list) - 1
	}
	if !r.isRange() {
		r.End = r.Start + 1
	}
	r.End++
	if r.Start > len(list) || r.End > len(list) {
		return nil, fmt.Errorf("index of out range")
	}
	return list[r.Start:r.End], nil
}

func (r Range) isRange() bool {
	return r.Start != r.End
}

func (r Range) isFull() bool {
	return r.Start == 0 && r.End == 0
}

func (r Range) isHalfOpen() bool {
	return (r.Start != 0 && r.End == 0) || (r.End != 0 && r.Start == 0)
}

func (r Range) isValid() bool {
	if r.isHalfOpen() || r.isFull() {
		return true
	}
	return r.Start < r.End
}

func getFilesFromSpec(spec string, es []os.DirEntry) ([]os.DirEntry, error) {
	rgl, err := parseSpec(spec)
	if err != nil {
		return nil, err
	}
	slices.Reverse(es)
	var files []os.DirEntry
	for _, r := range rgl {
		e, err := r.peek(es)
		if err != nil {
			return nil, err
		}
		files = append(files, e...)
	}
	slices.Reverse(files)
	return files, nil
}

func parseSpec(spec string) ([]Range, error) {
	if spec == "*" {
		var rg Range
		return []Range{rg}, nil
	}
	var list []Range
	for _, str := range strings.Split(spec, ",") {
		rg, err := parseSpecItem(str)
		if err != nil {
			return nil, err
		}
		list = append(list, rg)
	}
	return list, nil
}

func parseSpecItem(spec string) (Range, error) {
	if n, err := strconv.Atoi(spec); err == nil {
		r := Range{
			Start: n,
			End:   n,
		}
		return r, err
	}
	before, after, ok := strings.Cut(spec, "..")
	if !ok {
		return Range{}, fmt.Errorf("invalid spec format: missing range")
	}

	atoi := func(str string) (int, error) {
		v, err := strconv.Atoi(str)
		if err != nil && str != "" {
			return 0, err
		}
		if str != "" {
			v--
		}
		return v, nil
	}
	var (
		rg  Range
		err error
	)
	if rg.Start, err = atoi(before); err != nil {
		return rg, err
	}
	if rg.End, err = atoi(after); err != nil {
		return rg, err
	}
	if !rg.isValid() {
		return rg, fmt.Errorf("invalid range")
	}
	return rg, nil
}

type dsnInfo struct {
	User   string
	Pass   string
	Proto  string
	Host   string
	Port   int
	Name   string
	Driver string
}

func (d dsnInfo) Get() string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", d.User, d.Pass, d.Host, d.Port, d.Name)
}

func (d dsnInfo) Exec(qs []Migration) error {
	var err error
	switch d.Driver {
	case "mysql", "mariadb":
		err = d.execDefault(qs)
	case "":
		d.Driver = "dry"
		err = d.execDefault(qs)
	default:
		err = fmt.Errorf("%s: unsupported driver", d.Driver)
	}
	return err
}

func (d dsnInfo) execDefault(qs []Migration) error {
	db, err := sql.Open(d.Driver, d.Get())
	if err != nil {
		return err
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		return err
	}
	for i := range qs {
		switch qs[i].txMode {
		case TxDefault:
			err = d.execStmtsTxDefault(db, qs[i].Queries)
		case TxStmt:
			err = d.execStmtsTxQuery(db, qs[i].Queries)
		case TxOff:
			err = d.execStmtsTxOff(db, qs[i].Queries)
		default:
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (d dsnInfo) execStmtsTxQuery(db *sql.DB, queries []Unit) error {
	for _, q := range queries {
		tx, err := db.Begin()
		if err != nil {
			return err
		}
		if _, err = tx.Exec(q.Query); err == nil {
			tx.Commit()
		} else {
			tx.Rollback()
		}
		if err != nil && q.RollbackOnError() {
			return err
		}
	}
	return nil
}

func (d dsnInfo) execStmtsTxOff(db *sql.DB, queries []Unit) error {
	for _, q := range queries {
		_, err := db.Exec(q.Query)
		if err != nil && q.RollbackOnError() {
			return err
		}
	}
	return nil
}

func (d dsnInfo) execStmtsTxDefault(db *sql.DB, queries []Unit) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Commit()
	for _, q := range queries {
		_, err := tx.Exec(q.Query)
		if err != nil && q.RollbackOnError() {
			tx.Rollback()
			return err
		}
	}
	return nil
}

var errSupport = errors.New("not supported")

type dryRows struct{}

func (d dryRows) Columns() []string {
	return nil
}

func (d dryRows) Close() error {
	return nil
}

func (d dryRows) Next(dest []driver.Value) error {
	return io.EOF
}

type dryResult struct{}

func (d dryResult) LastInsertId() (int64, error) {
	return 0, nil
}

func (d dryResult) RowsAffected() (int64, error) {
	return 0, nil
}

type dryStmt struct {
	logger *slog.Logger
	stmt   string
}

func getStmt(logger *slog.Logger, stmt string) driver.Stmt {
	return dryStmt{
		logger: logger,
		stmt:   stmt,
	}
}

func (d dryStmt) Close() error {
	return nil
}

func (d dryStmt) NumInput() int {
	return 0
}

func (d dryStmt) Exec(args []driver.Value) (driver.Result, error) {
	vs := make([]driver.NamedValue, len(args))
	for i := range args {
		vs[i] = driver.NamedValue{
			Ordinal: i,
			Value:   args[i],
		}
	}
	return d.ExecContext(context.Background(), vs)
}

func (d dryStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	d.logger.Info("", "call", "exec", "sql", onelineSql(d.stmt), "args", len(args))
	var res dryResult
	return res, nil
}

func (d dryStmt) Query(args []driver.Value) (driver.Rows, error) {
	vs := make([]driver.NamedValue, len(args))
	for i := range args {
		vs[i] = driver.NamedValue{
			Ordinal: i,
			Value:   args[i],
		}
	}
	return d.QueryContext(context.Background(), vs)
}

func (d dryStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	d.logger.Info("", "call", "query", "sql", onelineSql(d.stmt), "args", len(args))
	var rows dryRows
	return rows, nil
}

type dryTx struct {
	logger *slog.Logger
}

func getTx(logger *slog.Logger) driver.Tx {
	return dryTx{
		logger: logger,
	}
}

func (d dryTx) Commit() error {
	d.logger.Info("", "call", "tx", "sql", "commit")
	return nil
}

func (d dryTx) Rollback() error {
	d.logger.Info("", "call", "tx", "sql", "rollback")
	return nil
}

type dryConn struct {
	logger *slog.Logger
}

func getConn(logger *slog.Logger) driver.Conn {
	return dryConn{
		logger: logger,
	}
}

func (d dryConn) Query(query string, args []driver.NamedValue) (driver.Rows, error) {
	return d.QueryContext(context.Background(), query, args)
}

func (d dryConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	d.logger.Info("", "call", "query", "sql", onelineSql(query), "args", len(args))
	var res dryRows
	return res, nil
}

func (d dryConn) Exec(query string, args []driver.NamedValue) (driver.Result, error) {
	return d.ExecContext(context.Background(), query, args)
}

func (d dryConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	d.logger.Info("", "call", "exec", "sql", onelineSql(query), "args", len(args))
	var res dryResult
	return res, nil
}

func (d dryConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	return getStmt(d.logger.WithGroup("stmt"), query), nil
}

func (d dryConn) Prepare(stmt string) (driver.Stmt, error) {
	return d.PrepareContext(context.Background(), stmt)
}

func (d dryConn) Close() error {
	return nil
}

func (d dryConn) Begin() (driver.Tx, error) {
	var opts driver.TxOptions
	return d.BeginTx(context.Background(), opts)
}

func (d dryConn) BeginTx(_ context.Context, opts driver.TxOptions) (driver.Tx, error) {
	d.logger.Info("", "call", "tx", "sql", "begin")
	return getTx(d.logger.WithGroup("tx")), nil
}

func (d dryConn) Ping(_ context.Context) error {
	d.logger.Info("", "call", "driver", "exec", "ping")
	return nil
}

type dryDriver struct {
	logger *slog.Logger
}

func getDriver() driver.Driver {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	return dryDriver{
		logger: logger,
	}
}

func (d dryDriver) Open(name string) (driver.Conn, error) {
	return getConn(d.logger.WithGroup("conn")), nil
}

func onelineSql(query string) string {
	var (
		scan  = bufio.NewScanner(strings.NewReader(query))
		lines []string
	)
	for scan.Scan() {
		lines = append(lines, strings.TrimSpace(scan.Text()))
	}
	return strings.Join(lines, " ")
}
