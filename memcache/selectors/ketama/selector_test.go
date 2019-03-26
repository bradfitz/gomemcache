package ketama

import (
	"fmt"
	"testing"
)

func BenchmarkPickServer(b *testing.B) {
	// at least two to avoid 0 and 1 special cases:
	benchPickServer(b, "127.0.0.1:1234", "127.0.0.1:1235")
}

func BenchmarkPickServer_Single(b *testing.B) {
	benchPickServer(b, "127.0.0.1:1234")
}

func benchPickServer(b *testing.B, servers ...string) {
	b.ReportAllocs()
	var ss ServerList
	ss.SetServers(servers...)
	for i := 0; i < b.N; i++ {
		if server, err := ss.PickServer("some key"); err != nil {
			fmt.Println(server)
			b.Fatal(err)
		}
	}
}
