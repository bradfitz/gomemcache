package memcache

import "testing"

func BenchmarkPickRoundRobinServer(b *testing.B) {
	// at least two to avoid 0 and 1 special cases:
	benchPickRoundRobinServer(b, "127.0.0.1:1234", "127.0.0.1:1235")
}

func BenchmarkPickRoundRobinServer_Single(b *testing.B) {
	benchPickRoundRobinServer(b, "127.0.0.1:1234")
}

func benchPickRoundRobinServer(b *testing.B, servers ...string) {
	b.ReportAllocs()
	var ss RoundRobinServerList
	ss.SetServers(servers...)
	for i := 0; i < b.N; i++ {
		if _, err := ss.PickServer("some key"); err != nil {
			b.Fatal(err)
		}
	}
}
