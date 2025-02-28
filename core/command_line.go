// Copyright 2025 openGemini Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"runtime/debug"
	"sort"
	"strings"
	"time"

	"github.com/olekukonko/tablewriter"
	"github.com/openGemini/opengemini-client-go/opengemini"
	"golang.org/x/term"

	"github.com/openGemini/openGemini-cli/geminiql"
	"github.com/openGemini/openGemini-cli/prompt"
)

type CommandLine struct {
	*CommandLineConfig
	httpClient HttpClient
	parser     geminiql.QLParser
	prompt     *prompt.Prompt

	executeAt time.Time
	timer     bool
	debug     bool
	suggest   bool
}

func NewCommandLine(cfg *CommandLineConfig) *CommandLine {
	httpClient, err := NewHttpClient(cfg)
	if err != nil {
		log.Fatal("create http client error: ", err)
	}
	var cl = &CommandLine{
		CommandLineConfig: cfg,
		parser:            geminiql.QLNewParser(),
		httpClient:        httpClient,
	}
	cl.prompt = prompt.NewPrompt(cl.executor)
	return cl
}

func (cl *CommandLine) executor(input string) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("panic recovered", r)
			fmt.Println("stack trace:")
			debug.PrintStack()
		}
	}()
	// no input nothing to do
	if input == "" {
		return
	}

	// input token to exit program
	if input == "quit" || input == "exit" || input == "\\q" {
		cl.prompt.Destruction(nil)
	}

	ast := &geminiql.QLAst{}
	lexer := geminiql.QLNewLexer(geminiql.NewTokenizer(strings.NewReader(input)), ast)
	cl.parser.Parse(lexer)

	cl.executeAt = time.Now()
	defer cl.elapse()

	var err error
	// parse token success
	if ast.Error == nil {
		err = cl.executeOnLocal(ast.Stmt)
	} else {
		err = cl.executeOnRemote(input)
	}
	if err != nil {
		fmt.Printf("error: %s\n", err)
	}
}

func (cl *CommandLine) elapse() {
	if !cl.timer {
		return
	}
	d := time.Since(cl.executeAt)
	fmt.Printf("Elapsed: %v\n", d)
}

func (cl *CommandLine) executeOnLocal(stmt geminiql.Statement) error {
	switch stmt := stmt.(type) {
	case *geminiql.UseStatement:
		return cl.executeUse(stmt)
	case *geminiql.PrecisionStatement:
		return cl.executePrecision(stmt)
	case *geminiql.HelpStatement:
		return cl.executeHelp(stmt)
	case *geminiql.AuthStatement:
		return cl.executeAuth(stmt)
	case *geminiql.TimerStatement:
		return cl.executeTimer(stmt)
	case *geminiql.DebugStatement:
		return cl.executeDebug(stmt)
	case *geminiql.PromptStatement:
		return cl.executePrompt(stmt)
	case *geminiql.InsertStatement:
		return cl.executeInsert(stmt)
	case *geminiql.ChunkedStatement:
		return errors.New("not implemented")
	case *geminiql.ChunkSizeStatement:
		return errors.New("not implemented")

	default:
		return fmt.Errorf("unsupport stmt %s", stmt)
	}
}

func (cl *CommandLine) executeOnRemote(s string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(cl.Timeout)*time.Millisecond)
	defer cancel()
	response, err := cl.httpClient.Query(ctx, &opengemini.Query{
		Database:        cl.Database,
		Precision:       opengemini.ToPrecision(cl.precision),
		RetentionPolicy: cl.RetentionPolicy,
		Command:         s,
	})
	if err != nil {
		return err
	}
	if response.Error != "" {
		return errors.New(response.Error)
	}
	for _, result := range response.Results {
		cl.output(result)
	}

	return nil
}

func (cl *CommandLine) output(result *opengemini.SeriesResult) {
	for _, series := range result.Series {
		if len(series.Columns) == 0 {
			continue
		}
		columnName := series.Columns[0]
		if columnName == "EXPLAIN ANALYZE" {
			cl.outputExplainAnalyze(series)
			continue
		}

		var tags []string
		for k, v := range series.Tags {
			tags = append(tags, fmt.Sprintf("%s=%s", k, v))
			sort.Strings(tags)
		}

		if series.Name != "" {
			_, _ = fmt.Fprintf(os.Stdout, "name: %s\n", series.Name)
		}
		if len(tags) != 0 {
			_, _ = fmt.Fprintf(os.Stdout, "tags: %s\n", strings.Join(tags, ", "))
		}

		writer := tablewriter.NewWriter(os.Stdout)
		cl.prettyTable(series, writer)
		writer.Render()
		_, _ = fmt.Fprintf(os.Stdout, "%d columns, %d rows in set\n", len(series.Columns), len(series.Values))
	}
}

func (cl *CommandLine) prettyTable(series *opengemini.Series, w *tablewriter.Table) {
	w.SetAutoFormatHeaders(false)
	w.SetHeader(series.Columns)
	for _, value := range series.Values {
		tuple := make([]string, len(value))
		for i, val := range value {
			// control time to int
			if len(series.Columns) > i && series.Columns[i] == "time" {
				iv, ok := val.(int64)
				if ok {
					tuple[i] = fmt.Sprintf("%d", iv)
					continue
				}
				fv, ok := val.(float64)
				if ok {
					tuple[i] = fmt.Sprintf("%.0f", fv)
					continue
				}
			}
			tuple[i] = fmt.Sprintf("%v", val)
		}
		w.Append(tuple)
	}
}

func (cl *CommandLine) outputExplainAnalyze(result *opengemini.Series) {
	var buff []string
	for _, value := range result.Values {
		for _, content := range value {
			s, ok := content.(string)
			if !ok {
				continue
			}
			buff = append(buff, s)
		}
	}
	_, _ = fmt.Fprintf(os.Stdout, "EXPLAIN ANALYZE\n---------------\n%s\n", strings.Join(buff, "\n"))
}

func (cl *CommandLine) Run() {
	cl.prompt.Run()
}

func (cl *CommandLine) executeUse(stmt *geminiql.UseStatement) error {
	cl.Database = stmt.DB
	if stmt.RP == "" {
		cl.RetentionPolicy = ""
	} else {
		cl.RetentionPolicy = stmt.RP
	}
	// show human retention policy
	var prettyRP = func() string {
		if cl.RetentionPolicy == "" {
			return "autogen"
		}
		return cl.RetentionPolicy
	}
	fmt.Printf("Using database: %s, retention policy: %s\n", cl.Database, prettyRP())
	return nil
}

func (cl *CommandLine) executePrecision(stmt *geminiql.PrecisionStatement) error {
	precision := strings.ToLower(stmt.Precision)
	switch precision {
	case "":
		cl.precision = "ns"
	case "h", "m", "s", "ms", "u", "ns", "rfc3339":
		cl.precision = precision
	default:
		return fmt.Errorf("unknown precision %q. precision must be rfc3339, h, m, s, ms, u or ns", precision)
	}
	return nil
}

func (cl *CommandLine) executeHelp(stmt *geminiql.HelpStatement) error {
	fmt.Println(
		`Usage:
  exit/quit/ctrl+d        quit the openGemini shell
  auth                    prompt for username and password
  use <db>[.rp]           set current database and optional retention policy
  precision <format>      specifies the format of the timestamp: rfc3339, h, m, s, ms, u or ns
  show cluster            show cluster node status information
  show users              show all existing users and their permission status
  show databases          show a list of all databases on the cluster
  show measurements       show measurement information on the database.retention_policy
  show series             show series information
  show tag keys           show tag key information
  show field keys         show field key information
  timer                   display execution time
  debug                   display http request interaction content
  prompt                  enable command line reminder and suggestion

  A full list of openGemini commands can be found at:
  https://docs.opengemini.org
	`)
	return nil
}

func (cl *CommandLine) executeAuth(stmt *geminiql.AuthStatement) error {
	fmt.Printf("username: ")
	_, _ = fmt.Scanf("%s\n", &cl.Username)
	fmt.Printf("password: ")
	password, _ := term.ReadPassword(int(os.Stdin.Fd()))
	fmt.Printf("\n")
	cl.Password = string(password)
	cl.httpClient.SetAuth(cl.Username, cl.Password)
	return nil
}

func (cl *CommandLine) executeTimer(stmt *geminiql.TimerStatement) error {
	// switch timer model enable or disable
	cl.timer = !cl.timer
	displayFlag := "disabled"
	if cl.timer {
		displayFlag = "enabled"
	}
	fmt.Printf("Timer is %s\n", displayFlag)
	return nil
}

func (cl *CommandLine) executeDebug(stmt *geminiql.DebugStatement) error {
	// switch debug model enable or disable
	cl.debug = !cl.debug
	displayFlag := "disabled"
	if cl.debug {
		displayFlag = "enabled"
	}
	fmt.Printf("Debug is %s\n", displayFlag)
	cl.httpClient.SetDebug(cl.debug)
	return nil
}

func (cl *CommandLine) executePrompt(stmt *geminiql.PromptStatement) error {
	// switch suggest model enable or disable
	cl.suggest = !cl.suggest
	displayFlag := "disabled"
	if cl.suggest {
		displayFlag = "enabled"
	}
	fmt.Printf("Prompt is %s\n", displayFlag)
	return nil
}

func (cl *CommandLine) executeInsert(stmt *geminiql.InsertStatement) error {
	return cl.httpClient.Write(context.Background(), cl.Database, cl.RetentionPolicy, stmt.LineProtocol, cl.precision)
}
