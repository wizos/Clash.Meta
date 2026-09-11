package geodata

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/metacubex/mihomo/component/geodata/router"
)

type testGeoDataLoader struct {
	domains   []*router.Domain
	calls     atomic.Int32
	started   chan struct{}
	release   chan struct{}
	startOnce sync.Once
}

func (l *testGeoDataLoader) LoadSiteByPath(_, _ string) ([]*router.Domain, error) {
	l.calls.Add(1)
	if l.started != nil {
		l.startOnce.Do(func() { close(l.started) })
		<-l.release
	}
	return l.domains, nil
}

func (*testGeoDataLoader) LoadSiteByBytes([]byte, string) ([]*router.Domain, error) {
	panic("unexpected LoadSiteByBytes call")
}

func (*testGeoDataLoader) LoadIPByPath(string, string) ([]*router.CIDR, error) {
	panic("unexpected LoadIPByPath call")
}

func (*testGeoDataLoader) LoadIPByBytes([]byte, string) ([]*router.CIDR, error) {
	panic("unexpected LoadIPByBytes call")
}

func TestLoadGeoSiteMatcherCachesCompleteExpression(t *testing.T) {
	loader := &testGeoDataLoader{domains: testGeoSiteDomains()}
	useTestGeoSiteLoader(t, loader)

	for i := 0; i < 2; i++ {
		matcher, err := LoadGeoSiteMatcher("test@alpha@beta")
		if err != nil {
			t.Fatal(err)
		}
		assertBothAttributesOnly(t, matcher)
	}

	if calls := loader.calls.Load(); calls != 1 {
		t.Fatalf("same expression loaded geosite %d times, want 1", calls)
	}
}

func TestLoadGeoSiteMatcherReloadsListForAttributeOrder(t *testing.T) {
	loader := &testGeoDataLoader{domains: testGeoSiteDomains()}
	useTestGeoSiteLoader(t, loader)

	for _, expression := range []string{"test@alpha@beta", "test@beta@alpha"} {
		matcher, err := LoadGeoSiteMatcher(expression)
		if err != nil {
			t.Fatal(err)
		}
		assertBothAttributesOnly(t, matcher)
	}

	if calls := loader.calls.Load(); calls != 2 {
		t.Fatalf("different expressions loaded geosite %d times, want 2", calls)
	}
}

func TestLoadGeoSiteMatcherCoalescesConcurrentListLoads(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	loader := &testGeoDataLoader{
		domains: testGeoSiteDomains(),
		started: started,
		release: release,
	}
	useTestGeoSiteLoader(t, loader)

	type result struct {
		matcher router.DomainMatcher
		err     error
	}
	load := func(expression string) <-chan result {
		resultCh := make(chan result, 1)
		go func() {
			matcher, err := LoadGeoSiteMatcher(expression)
			resultCh <- result{matcher: matcher, err: err}
		}()
		return resultCh
	}

	first := load("test@alpha@beta")
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for the first geosite load")
	}
	second := load("test@beta@alpha")
	time.Sleep(20 * time.Millisecond)
	close(release)

	for _, resultCh := range []<-chan result{first, second} {
		got := <-resultCh
		if got.err != nil {
			t.Fatal(got.err)
		}
		assertBothAttributesOnly(t, got.matcher)
	}
	if calls := loader.calls.Load(); calls != 1 {
		t.Fatalf("concurrent expressions loaded geosite %d times, want 1", calls)
	}
}

func useTestGeoSiteLoader(t *testing.T, loader LoaderImplementation) {
	t.Helper()
	oldLoaderName, oldSiteMatcher := geoLoaderName, geoSiteMatcher
	loaderName := "test:" + t.Name()
	oldCreator, hadCreator := loaders[loaderName]
	RegisterGeoDataLoaderImplementationCreator(loaderName, func() LoaderImplementation {
		return loader
	})
	geoLoaderName = loaderName
	geoSiteMatcher = "succinct"
	ClearGeoSiteCache()
	t.Cleanup(func() {
		ClearGeoSiteCache()
		geoLoaderName = oldLoaderName
		geoSiteMatcher = oldSiteMatcher
		if hadCreator {
			loaders[loaderName] = oldCreator
		} else {
			delete(loaders, loaderName)
		}
	})
}

func testGeoSiteDomains() []*router.Domain {
	attribute := func(key string) *router.Domain_Attribute {
		return &router.Domain_Attribute{Key: key}
	}
	return []*router.Domain{
		{
			Type:      router.Domain_Full,
			Value:     "both.example",
			Attribute: []*router.Domain_Attribute{attribute("alpha"), attribute("beta")},
		},
		{
			Type:      router.Domain_Full,
			Value:     "alpha.example",
			Attribute: []*router.Domain_Attribute{attribute("alpha")},
		},
	}
}

func assertBothAttributesOnly(t *testing.T, matcher router.DomainMatcher) {
	t.Helper()
	if !matcher.ApplyDomain("both.example") {
		t.Error("matcher did not include the domain with both attributes")
	}
	if matcher.ApplyDomain("alpha.example") {
		t.Error("matcher included a domain missing one requested attribute")
	}
}
