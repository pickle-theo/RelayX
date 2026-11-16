package postgres

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"relayx/s2/internal/facts"
	"relayx/s2/internal/store"
)

// 设置 S2_TEST_DSN 可运行真实数据库契约测试，例如：
// DSN 示例：postgres://relayx:relayx@127.0.0.1:55432/relayx_phase6?sslmode=disable
func TestPostgresStoreContract(t *testing.T) {
	base := os.Getenv("S2_TEST_DSN")
	if base == "" {
		t.Skip("S2_TEST_DSN not set")
	}
	ctx := context.Background()
	setup, err := Open(ctx, base, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := setup.pool.Exec(ctx, `DROP SCHEMA IF EXISTS s2_test CASCADE; CREATE SCHEMA s2_test`); err != nil {
		t.Fatal(err)
	}
	setup.Close()

	sep := "?"
	if strings.Contains(base, "?") {
		sep = "&"
	}
	migration := filepath.Join("..", "..", "..", "migrations", "0001_reliable_core.sql")
	st, err := Open(ctx, base+sep+"search_path=s2_test", migration)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	m := &store.Message{
		ID: "m1", TenantID: "t1", ProducerID: "u:alice", ProducerKind: store.ProducerUser,
		SourceDeviceID: "phone", ClientMsgID: "c1", Payload: []byte("x"), PayloadSize: 1,
		DeliveryMode: store.ModeAllEndpoints, TTLSeconds: 60, MaxAttempts: 3,
		PlatformState: store.MessageAccepted,
	}
	var version int
	if err := st.WithinTx(ctx, func(tx store.Tx) error {
		if _, _, err := tx.Messages().Insert(ctx, tx, m); err != nil {
			return err
		}
		if _, err := tx.Messages().MarkFanoutDone(ctx, tx, m.ID); err != nil {
			return err
		}
		if err := tx.Deliveries().InsertBatch(ctx, tx, []*store.Delivery{{
			ID: "d1", TenantID: "t1", MessageID: "m1", UserID: "bob", DeviceID: "phone",
			EndpointID: "bob:phone", DeliveryState: store.DeliverySent, Attempts: 1,
		}}); err != nil {
			return err
		}
		ds, err := tx.Deliveries().ListByMessage(ctx, "m1")
		if err != nil || len(ds) != 1 {
			t.Fatalf("deliveries=%d err=%v", len(ds), err)
		}
		d := ds[0]
		next := int32(1)
		v, hit, err := tx.Deliveries().MarkSent(ctx, tx, d.ID, next, 1)
		if err != nil || !hit {
			t.Fatalf("MarkSent hit=%v err=%v", hit, err)
		}
		version = v
		return tx.Facts().InsertBatch(ctx, tx, []*store.Fact{{
			TenantID: "t1", MessageID: "m1", EndpointID: ptr("bob:phone"),
			FactType: store.FactDeliverySent, StateVersion: version,
		}})
	}); err != nil {
		t.Fatal(err)
	}
	if version == 0 {
		t.Fatal("MarkSent returned zero version")
	}

	if err := st.WithinTx(ctx, func(tx store.Tx) error {
		ok, err := tx.Deliveries().Transition(ctx, tx, "d1",
			[]string{store.DeliverySent}, store.DeliveryReceiptAcked,
			store.DeliveryPatch{ClearNextAttemptAt: true}, version)
		if err != nil || !ok {
			t.Fatalf("Transition hit=%v err=%v", ok, err)
		}
		res, hit, err := tx.Messages().SettleIfEligible(ctx, tx, "m1")
		if err != nil || !hit {
			t.Fatalf("settle hit=%v err=%v", hit, err)
		}
		if res.Counts.Total != 1 || res.Counts.ReceiptAcked != 1 {
			t.Fatalf("counts=%+v", res.Counts)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	fs, err := st.Facts().ListByMessage(ctx, "m1")
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, f := range fs {
		if f.FactType == store.FactDeliverySent {
			found = f.EventID == facts.Derive("t1", "m1", store.FactDeliverySent, ptr("bob:phone"), version)
		}
	}
	if !found {
		t.Fatalf("facts=%+v", fs)
	}
	got, _ := st.Messages().GetByID(ctx, "m1")
	if got.PlatformState != store.MessageSettled || got.SettledAt == nil {
		t.Fatalf("message=%+v", got)
	}
	if _, hit, _ := st.Messages().ExpireIfEligible(ctx, nil, "m1"); hit {
		t.Fatal("SETTLED must not be expired")
	}
}

func ptr(s string) *string { return &s }
