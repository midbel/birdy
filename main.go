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
	var (
		dry bool
		dsn dsnInfo
	)
	flag.BoolVar(&dry, "n", false, "dry run")
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
	switch cmd := flag.Arg(0); cmd {
	case "up", "down", "redo":
		list, err := extractSQL(file, spec)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
		if err := execute(dsn, cmd, list, dry); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
	case "status":
		// pass
	default:
		fmt.Fprintf(os.Stderr, "unknown command %s", cmd)
		fmt.Fprintln(os.Stderr)
	}
}

func extractSQL(file, spec string) ([]Migration, error) {
	s := createSplitter()
	return s.Load(file, spec)
}

func execute(dsn dsnInfo, cmd string, all []Migration, dry bool) error {
	db, err := sql.Open(dsn.Driver, dsn.Get())
	if err != nil {
		return err
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		return err
	}
	for _, a := range all {
		list := a.Up
		if cmd == "down" {
			list = a.Down
		} else if cmd == "redo" {
			list = append(a.Down, a.Up...)
		}
		if err := executeStmts(db, list); err != nil {
			return err
		}
	}
	return nil

}

func executeStmts(db *sql.DB, queries []Unit) error {
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

type Unit struct {
	Query string
	Group int
	Up    bool
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
		if len(sql) == 0 {
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
	} else if sql == "down" {
		s.up = false
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

type Range struct {
	Start int
	End   int
}

func (r Range) peek(list []os.DirEntry) ([]os.DirEntry, error) {
	if r.End == 0 && r.Start != 0 {
		r.End = len(list) - 1
	}
	if !r.isRange() {
		r.End = r.Start + 1
	}
	if r.Start > len(list) || r.End > len(list) {
		return nil, fmt.Errorf("index of out range")
	}
	return list[r.Start:r.End], nil
}

func (r Range) isRange() bool {
	return r.Start != r.End
}

func (r Range) isValid() bool {
	return r.Start <= r.End
}

func parseSpec(spec string) ([]Range, error) {
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
	bef, aft, ok := strings.Cut(spec, "..")
	if !ok {
		return Range{}, fmt.Errorf("invalid spec format: missing range")
	}
	var (
		rg  Range
		err error
	)
	if rg.Start, err = strconv.Atoi(bef); err != nil && bef != "" {
		return rg, err
	}
	rg.Start--
	if rg.End, err = strconv.Atoi(aft); err != nil && aft != "" {
		return rg, err
	}
	rg.End--
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
