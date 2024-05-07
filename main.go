package main

import (
	"bufio"
	"bytes"
	"database/sql"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

func main() {
	var dsn dsnInfo
	flag.StringVar(&dsn.User, "u", os.Getenv("username"), "database username")
	flag.StringVar(&dsn.Pass, "w", os.Getenv("password"), "database user password")
	flag.StringVar(&dsn.Host, "s", os.Getenv("server"), "server name")
	flag.IntVar(&dsn.Port, "p", 0, "server port")
	flag.StringVar(&dsn.Name, "d", os.Getenv("database"), "database name")
	flag.StringVar(&dsn.Driver, "i", os.Getenv("driver"), "database driver")
	flag.Parse()
	var (
		file string
		spec string = "1"
	)
	if flag.NArg() == 2 {
		file = flag.Arg(1)
	} else if flag.NArg() == 3 {
		file = flag.Arg(2)
		spec = flag.Arg(1)

	} else {
		fmt.Fprintln(os.Stderr, "invalid number of arguments given")
		os.Exit(2)
	}
	list, err := extractSQL(file, spec)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	switch cmd := flag.Arg(0); cmd {
	case "up", "down", "redo":
		units := getUnitsFromCommand(cmd, list)
		err = dsn.Exec(units)
	case "info":
		for _, m := range list {
			n, _ := fmt.Fprintf(os.Stdout, "migration #%d", m.Group)
			fmt.Fprintln(os.Stdout)
			fmt.Fprintln(os.Stdout, strings.Repeat("=", n))
			fmt.Fprintf(os.Stdout, "- up  : %d queries", len(m.Up))
			fmt.Fprintln(os.Stdout)
			fmt.Fprintf(os.Stdout, "- down: %d queries", len(m.Down))
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

func extractSQL(file, spec string) ([]Migration, error) {
	s := createSplitter()
	return s.Load(file, spec)
}

func getUnitsFromCommand(cmd string, all []Migration) [][]Unit {
	var list [][]Unit
	for _, a := range all {
		qs := a.Up
		if cmd == "down" {
			qs = a.Down
		} else if cmd == "redo" {
			qs = append(a.Down, qs...)
		}
		list = append(list, qs)
	}
	return list
}

type Unit struct {
	Query string
	Group int
	Up    bool
}

func group(units []Unit) []Migration {
	var ms []Migration
	for _, u := range units {
		x := slices.IndexFunc(ms, func(m Migration) bool {
			return m.Group == u.Group
		})
		if x < 0 {
			m := Migration{
				Group: u.Group,
			}
			ms = append(ms, m)
			x = len(ms) - 1
		}
		if u.Up {
			ms[x].Up = append(ms[x].Up, u)
		} else {
			ms[x].Down = append(ms[x].Down, u)
		}
	}
	return ms
}

type Migration struct {
	Up    []Unit
	Down  []Unit
	Group int
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
}

func createSplitter() *splitter {
	return &splitter{
		delimiter: []byte{semi},
		up:        true,
	}
}

func (s *splitter) Load(file, spec string) ([]Migration, error) {
	es, err := os.ReadDir(file)
	if err != nil {
		units, err := s.load(file)
		if err != nil {
			return nil, err
		}
		return group(units), nil
	}
	slices.Reverse(es)
	rgl, err := parseSpec(spec)
	if err != nil {
		return nil, err
	}

	var files []os.DirEntry
	for _, r := range rgl {
		e, err := r.peek(es)
		if err != nil {
			return nil, err
		}
		files = append(files, e...)
	}

	var all []Migration
	for _, e := range files {
		units, err := s.load(filepath.Join(file, e.Name()))
		if err != nil {
			return nil, err
		}
		all = append(all, group(units)...)
	}
	return all, nil
}

func (s *splitter) load(file string) ([]Unit, error) {
	r, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	s.reset()
	return s.Split(r)
}

func (s *splitter) Split(r io.Reader) ([]Unit, error) {
	var (
		scan = bufio.NewScanner(r)
		list []Unit
	)
	scan.Split(s.splitStmt)
	for scan.Scan() {
		sql := scan.Text()
		if !s.isRegularDelimiter() {
			sql = strings.TrimRight(sql, string(s.delimiter))
			sql = strings.TrimSpace(sql)
		}
		if s.updateState(sql) {
			continue
		}
		if len(sql) == 0 || s.ignore {
			continue
		}
		u := Unit{
			Query: sql,
			Up:    s.up,
			Group: s.group,
		}
		list = append(list, u)
	}
	return list, scan.Err()
}

func (s *splitter) updateState(sql string) bool {
	s.revertDelimiter()
	if !strings.HasPrefix(sql, "--") {
		return false
	}
	sql = strings.TrimPrefix(sql, "--")
	sql = strings.TrimSpace(sql)
	if sql == "up" {
		s.up = true
		s.group++
		s.ignore = false
	} else if sql == "down" {
		s.up = false
		s.ignore = false
	} else if sql == "ignore" {
		s.ignore = true
	} else if strings.HasPrefix(sql, "delimiter") {
		sql = strings.TrimPrefix(sql, "delimiter")
		sql = strings.TrimSpace(sql)
		s.delimiter = []byte(sql)
	} else {
		// skip SQL parameters - useless
		return false
	}
	return true
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

func (d dsnInfo) Exec(queries [][]Unit) error {
	var err error
	switch d.Driver {
	case "mysql", "mariadb":
		err = d.execMysql(queries)
	default:
		err = fmt.Errorf("%s: unsupported driver", d.Driver)
	}
	return err
}

func (d dsnInfo) execMysql(queries [][]Unit) error {
	db, err := sql.Open(d.Driver, d.Get())
	if err != nil {
		return err
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		return err
	}
	for i := range queries {
		if err := d.execStmts(db, queries[i]); err != nil {
			return err
		}
	}
	return nil
}

func (d dsnInfo) execStmts(db *sql.DB, queries []Unit) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Commit()
	for _, q := range queries {
		_, err := tx.Exec(q.Query)
		if err != nil {
			tx.Rollback()
			return err
		}
		fmt.Fprintln(os.Stdout, q.Query)
	}
	return nil
}
