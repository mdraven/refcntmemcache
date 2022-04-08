package refcntmemcache_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/mdraven/refcntmemcache"
	"github.com/stretchr/testify/assert"
)

type testType struct {
	value string
}

func TestBasicSetGetPut(t *testing.T) {
	cache := refcntmemcache.New[int, testType](time.Second, 10)

	handle1, new := cache.Set(1, &testType{"value1"})
	assert.True(t, new)
	assert.Equal(t, "value1", handle1.Get().value)

	handle1.Put()
	assert.Nil(t, cache.Close())
}

func TestCloseWithoutPut(t *testing.T) {
	cache := refcntmemcache.New[int, testType](time.Second, 10)

	cache.Set(1, &testType{"value1"})

	assert.NotNil(t, cache.Close())
}

func TestDoublePut(t *testing.T) {
	cache := refcntmemcache.New[int, testType](time.Second, 10)

	handle1, _ := cache.Set(1, &testType{"value1"})

	handle1.Put()
	handle1.Put()
	assert.Nil(t, cache.Close())
}

func TestGetAfterPut(t *testing.T) {
	cache := refcntmemcache.New[int, testType](time.Second, 10)

	handle1, _ := cache.Set(1, &testType{"value1"})

	handle1.Put()

	assert.Nil(t, handle1.Get())
}

func TestDoubleSet(t *testing.T) {
	cache := refcntmemcache.New[int, testType](time.Second, 10)

	data := &testType{"value1"}

	handle1, new := cache.Set(1, data)
	assert.True(t, new)

	handle2, new := cache.Set(1, &testType{"value2"})
	assert.False(t, new)

	assert.Equal(t, data, handle1.Get())
	assert.Equal(t, data, handle2.Get())
}

func TestRemoveByDuration(t *testing.T) {
	cache := refcntmemcache.New[int, testType](time.Millisecond*100, 10)

	data := &testType{"value1"}

	handle1, _ := cache.Set(1, data)
	assert.NotNil(t, handle1.Get())
	handle1.Put()

	time.Sleep(time.Millisecond * 200)

	handle2 := cache.Get(1)
	assert.Nil(t, handle2.Get())
}

func TestRemoveByMaxElements(t *testing.T) {
	cache := refcntmemcache.New[int, testType](time.Duration(0), 2)

	data := &testType{"value1"}

	handle1, _ := cache.Set(1, data)
	assert.NotNil(t, handle1.Get())

	handle2, _ := cache.Set(2, data)
	assert.NotNil(t, handle2.Get())

	handle2.Put()
	handle1.Put()

	handle3, _ := cache.Set(3, data)
	assert.NotNil(t, handle3.Get())

	handle4 := cache.Get(1)
	assert.Nil(t, handle4.Get())

	handle5 := cache.Get(2)
	assert.NotNil(t, handle5.Get())
}

func TestFreeFunc(t *testing.T) {
	cache := refcntmemcache.New[int, testType](time.Millisecond*100, 10)

	free := make(chan string, 1)

	cache.SetFree(func(value *testType) {
		free <- value.value
	})

	data := &testType{"value1"}

	handle1, _ := cache.Set(1, data)
	assert.NotNil(t, handle1.Get())
	handle1.Put()

	time.Sleep(time.Millisecond * 200)

	assert.Equal(t, data.value, <-free)
}

func TestFreeHandleInFreeFun(t *testing.T) {
	cache := refcntmemcache.New[int, testType](time.Millisecond*100, 10)

	data := &testType{"value1"}

	free := make(chan *testType)
	cache.SetFree(func(value *testType) {
		handle3 := cache.Get(2)
		free <- handle3.Get()
		handle3.Put()
	})

	handle1, _ := cache.Set(1, data)
	handle2, _ := cache.Set(2, data)

	handle1.Put()

	time.Sleep(time.Millisecond * 200)

	assert.Equal(t, data, <-free)

	handle2.Put()

	time.Sleep(time.Millisecond * 200)

	assert.Nil(t, <-free)
}

func TestCheckingCounter(t *testing.T) {
	cache := refcntmemcache.New[int, testType](time.Millisecond*100, 10)

	data := &testType{"value1"}

	handle1, _ := cache.Set(1, data)
	assert.NotNil(t, handle1.Get())

	time.Sleep(time.Millisecond * 300)

	handle2 := cache.Get(1)
	assert.NotNil(t, handle2.Get())
}

func TestRemoveOrderByTime1(t *testing.T) {
	cache := refcntmemcache.New[int, testType](time.Millisecond*100, 10)

	free := make(chan string, 10)
	cache.SetFree(func(value *testType) {
		free <- value.value
	})

	handle1, _ := cache.Set(1, &testType{"value1"})
	time.Sleep(time.Millisecond * 10) // чтобы time у handle1 и handle2 отличались
	handle2, _ := cache.Set(2, &testType{"value2"})
	time.Sleep(time.Millisecond * 10) // чтобы time у handle2 и handle3 отличались
	handle3, _ := cache.Set(3, &testType{"value3"})

	handle2.Put()
	handle1.Put()
	handle3.Put()

	assert.Equal(t, "value1", <-free)
	assert.Equal(t, "value2", <-free)
	assert.Equal(t, "value3", <-free)
}

func TestRemoveOrderByTime2(t *testing.T) {
	cache := refcntmemcache.New[int, testType](time.Millisecond*100, 10)

	free := make(chan string, 10)
	cache.SetFree(func(value *testType) {
		free <- value.value
	})

	handle1, _ := cache.Set(1, &testType{"value1"})
	time.Sleep(time.Millisecond * 10) // чтобы time у handle1 и handle2 отличались
	handle2, _ := cache.Set(2, &testType{"value2"})
	time.Sleep(time.Millisecond * 10) // чтобы time у handle2 и handle3 отличались
	handle3, _ := cache.Set(3, &testType{"value3"})

	handle3.Put()
	handle1.Put()
	handle2.Put()

	assert.Equal(t, "value1", <-free)
	assert.Equal(t, "value2", <-free)
	assert.Equal(t, "value3", <-free)
}

func TestRemoveOrderByMaxElements(t *testing.T) {
	cache := refcntmemcache.New[int, testType](time.Duration(0), 1)

	free := make(chan string, 10)
	cache.SetFree(func(value *testType) {
		free <- value.value
	})

	cache.Set(333, &testType{""}) // Этот элемент нужен чтобы занимать место в cache

	handle1, _ := cache.Set(1, &testType{"value1"})
	time.Sleep(time.Millisecond * 10) // чтобы time у handle1 и handle2 отличались
	handle2, _ := cache.Set(2, &testType{"value2"})
	time.Sleep(time.Millisecond * 10) // чтобы time у handle2 и handle3 отличались
	handle3, _ := cache.Set(3, &testType{"value3"})

	handle2.Put()
	handle1.Put()
	handle3.Put()

	assert.Equal(t, "value2", <-free)
	assert.Equal(t, "value1", <-free)
	assert.Equal(t, "value3", <-free)
}

func TestThreadSafeTest(t *testing.T) {
	cache := refcntmemcache.New[int, testType](time.Duration(0), 1)

	start := sync.WaitGroup{}
	result := make(chan bool)
	wait := sync.WaitGroup{}

	numGoroutines := 4
	numValues := 100

	start.Add(1)
	for i := 0; i < numGoroutines; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()

			start.Wait()

			for i := 0; i < numValues; i++ {
				value := fmt.Sprintf("value%d", i)

				handle1, _ := cache.Set(i, &testType{value})
				defer handle1.Put()

				handle2 := cache.Get(i)
				defer handle2.Put()
				result <- handle2.Get().value == value
			}
		}()
	}

	start.Done()
	for i := 0; i < numGoroutines*numValues; i++ {
		assert.True(t, <-result)
	}

	wait.Wait()

	assert.Nil(t, cache.Close())
}

func TestMove(t *testing.T) {
	cache := refcntmemcache.New[int, testType](time.Millisecond*100, 10)

	func1 := func() refcntmemcache.Handle[testType] {
		handle1, _ := cache.Set(1, &testType{"value1"})
		defer handle1.Put()

		handle2 := handle1.Move()
		defer handle2.Put()

		assert.Nil(t, handle1.Get())
		assert.Equal(t, "value1", handle2.Get().value)

		return handle2.Move()
	}

	handle1 := func1()
	assert.Equal(t, "value1", handle1.Get().value)

	handle1.Put()

	time.Sleep(time.Millisecond * 200)

	handle2 := cache.Get(1)
	assert.Nil(t, handle2.Get())
}

func TestCopy(t *testing.T) {
	cache := refcntmemcache.New[int, testType](time.Millisecond*100, 10)

	func1 := func() refcntmemcache.Handle[testType] {
		handle1, _ := cache.Set(1, &testType{"value1"})
		defer handle1.Put()

		handle2 := handle1.Copy()
		defer handle2.Put()

		assert.Equal(t, "value1", handle1.Get().value)
		assert.Equal(t, "value1", handle2.Get().value)

		return handle2.Copy()
	}

	handle1 := func1()
	assert.Equal(t, "value1", handle1.Get().value)

	handle1.Put()

	time.Sleep(time.Millisecond * 200)

	handle2 := cache.Get(1)
	assert.Nil(t, handle2.Get())
}

func TestMapMove(t *testing.T) {
	cache := refcntmemcache.New[int, testType](time.Millisecond*100, 10)

	handle1, _ := cache.Set(1, &testType{"value1"})
	defer handle1.Put()

	handle2, _ := refcntmemcache.MoveMap(&handle1, func(v *testType) (*string, error) {
		return &v.value, nil
	})
	defer handle2.Put()

	assert.Nil(t, handle1.Get())
	assert.Equal(t, "value1", *handle2.Get())

	handle2.Put()

	time.Sleep(time.Millisecond * 200)

	assert.Nil(t, handle2.Get())
	assert.Nil(t, cache.Close())
}

func TestMapCopy(t *testing.T) {
	cache := refcntmemcache.New[int, testType](time.Millisecond*100, 10)

	handle1, _ := cache.Set(1, &testType{"value1"})
	defer handle1.Put()

	handle2, _ := refcntmemcache.CopyMap(&handle1, func(v *testType) (*string, error) {
		return &v.value, nil
	})
	defer handle2.Put()

	assert.Equal(t, "value1", handle1.Get().value)
	assert.Equal(t, "value1", *handle2.Get())

	handle2.Put()

	time.Sleep(time.Millisecond * 200)

	assert.NotNil(t, cache.Close())

	handle1.Put()

	time.Sleep(time.Millisecond * 200)

	assert.Nil(t, handle2.Get())
	assert.Nil(t, cache.Close())
}
