// maintenance 命令执行 S2 状态修复操作。S8 将扩展
// 保留期、投影重建与事实发布控制。
package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"relayx/s2/internal/store/postgres"
)

func main() {
	dsn := flag.String("dsn", "", "PostgreSQL DSN")
	migration := flag.String("migration", "migrations/0001_reliable_core.sql", "migration file")
	action := flag.String("action", "reset", "reset | drift-check")
	flag.Parse()
	if *dsn == "" {
		fmt.Fprintln(os.Stderr, "-dsn is required")
		os.Exit(2)
	}

	ctx := context.Background()
	st, err := postgres.Open(ctx, *dsn, *migration)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer st.Close()

	switch *action {
	case "reset":
		if err := st.Endpoints().ResetAllOffline(ctx); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		fmt.Println("all endpoints set OFFLINE")
	case "drift-check":
		// 没有存活的网关进程就没有会话权威，因此
		// 所有 ONLINE 行按定义都是过期的。
		bad, err := st.Endpoints().ListInconsistentOnline(ctx, func(string, string, string) bool { return false })
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		for _, e := range bad {
			if err := st.Endpoints().MarkOffline(ctx, e.TenantID, e.UserID, e.DeviceID, ""); err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
		}
		fmt.Printf("corrected %d stale endpoints\n", len(bad))
	default:
		fmt.Fprintf(os.Stderr, "unknown action %q\n", *action)
		os.Exit(2)
	}
}
