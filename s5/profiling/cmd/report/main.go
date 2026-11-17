// Command report 是 harness.sh 的结构化报告入口：
//
//	report -mode=parse    -in=bench.txt -out=reports/baseline.json -label=baseline
//	report -mode=compare  -before=reports/baseline.json -after=reports/after.json -out=reports/compare.md
//	report -mode=profiles -dir=profiling/profiles -pkg=./benchmark -benchtime=1s
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"

	"relayx/s5/benchmark"
	"relayx/s5/profiling"
)

func main() {
	mode := flag.String("mode", "parse", "parse | compare | profiles")
	in := flag.String("in", "", "parse: go test -bench 文本输出路径")
	out := flag.String("out", "", "输出路径（parse: JSON；compare: Markdown）")
	label := flag.String("label", "baseline", "报告标签：baseline | after")
	beforePath := flag.String("before", "", "compare: 基线 JSON")
	afterPath := flag.String("after", "", "compare: 优化后 JSON")
	dir := flag.String("dir", "profiling/profiles", "profiles: 归档目录")
	pkg := flag.String("pkg", "./benchmark", "profiles: 基准包")
	benchPattern := flag.String("bench", "Benchmark", "profiles: 基准名过滤")
	benchTime := flag.Duration("benchtime", 0, "profiles: 单基准时长（默认 1s）")
	flag.Parse()

	switch *mode {
	case "parse":
		if *in == "" || *out == "" {
			fail("parse 需要 -in 与 -out")
		}
		benches, err := profiling.ParseBenchFile(*in)
		if err != nil {
			fail(err.Error())
		}
		benches = medianBySuffix(benches)
		rep := benchmark.NewReport(*label, benches)
		if err := rep.WriteFile(*out); err != nil {
			fail(err.Error())
		}
		fmt.Printf("parsed %d benchmarks → %s (commit %s)\n", len(benches), *out, rep.Commit)

	case "ab":
		if *in == "" {
			fail("ab 需要 -in（parse 生成的 JSON）")
		}
		rep, err := benchmark.ReadReport(*in)
		if err != nil {
			fail(err.Error())
		}
		md := profiling.ABTable(rep)
		if *out == "" {
			fmt.Print(md)
			return
		}
		if err := profiling.WriteCompare(*out, md); err != nil {
			fail(err.Error())
		}
		fmt.Printf("ab table written → %s\n", *out)

	case "compare":
		if *beforePath == "" || *afterPath == "" {
			fail("compare 需要 -before 与 -after")
		}
		before, err := benchmark.ReadReport(*beforePath)
		if err != nil {
			fail(err.Error())
		}
		after, err := benchmark.ReadReport(*afterPath)
		if err != nil {
			fail(err.Error())
		}
		md := profiling.Compare(before, after)
		if *out == "" {
			fmt.Print(md)
			return
		}
		if err := profiling.WriteCompare(*out, md); err != nil {
			fail(err.Error())
		}
		fmt.Printf("compare written → %s\n", *out)

	case "profiles":
		paths, err := profiling.RunBenchProfiles(context.Background(), *dir, *pkg, *benchPattern, *benchTime)
		if err != nil {
			fail(err.Error())
		}
		for _, p := range paths {
			fmt.Println(p)
		}

	default:
		fail("unknown mode: " + *mode)
	}
}

func fail(msg string) {
	fmt.Fprintln(os.Stderr, "report:", msg)
	os.Exit(1)
}

// medianBySuffix 把 `-count=3` 产生的重复行折叠成一行（取中位数）。
// 报告只归档一个确定值，避免「挑最好的一次」。
func medianBySuffix(reports []benchmark.BenchReport) []benchmark.BenchReport {
	groups := make(map[string][]benchmark.BenchReport)
	order := make([]string, 0, len(reports))
	for _, r := range reports {
		if _, ok := groups[r.Name]; !ok {
			order = append(order, r.Name)
		}
		groups[r.Name] = append(groups[r.Name], r)
	}
	out := make([]benchmark.BenchReport, 0, len(groups))
	for _, name := range order {
		rows := groups[name]
		sort.Slice(rows, func(i, j int) bool { return rows[i].NsPerOp < rows[j].NsPerOp })
		mid := rows[len(rows)/2]
		out = append(out, mid)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}
