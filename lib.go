package refcntmemcache

import (
	"errors"
	"sync"
	"time"
)

var ErrRefIsNotZero = errors.New("reference counter is not zero")
var ErrEmptyHandle = errors.New("handle is nil")

type command int

const (
	stopProcessDeletedValues     command = 1
	listOfDeletedValuesIsChanged command = 2
)

// Без этого интерфейса придётся или тащить Key в Handle или Mutex в каждый cacheDatum
type keylessRefCntMemCache[Value any] interface {
	put(handle *Handle[Value])
	copy(handle *Handle[Value]) Handle[Value]
}

type listLinks[Value any] struct {
	next *cacheDatum[Value]
	prev *cacheDatum[Value]
}

type cacheDatum[Value any] struct {
	counter        uint64
	key            interface{}
	valuesToDelete listLinks[Value] // Список элементов которые можно удалять. Упорядочено по time, если lifetime > 0.
	time           time.Time
	value          *Value
}

// Извлечь из списка
func (cd *cacheDatum[Value]) extractFromList(first, last **cacheDatum[Value]) {
	if cd.valuesToDelete.prev != nil {
		cd.valuesToDelete.prev.valuesToDelete.next = cd.valuesToDelete.next
	}
	if cd.valuesToDelete.next != nil {
		cd.valuesToDelete.next.valuesToDelete.prev = cd.valuesToDelete.prev
	}

	if *first == cd {
		*first = cd.valuesToDelete.next
	}
	if *last == cd {
		*last = cd.valuesToDelete.prev
	}

	cd.valuesToDelete.next = nil
	cd.valuesToDelete.prev = nil
}

// Добавить в список. sortByTime чтобы time было упорядочено по возрастанию.
func (cd *cacheDatum[Value]) insertToList(first, last **cacheDatum[Value], sortByTime bool) {
	if *first != nil {
		if !sortByTime || (*first).time.After(cd.time) {
			cd.valuesToDelete.next = *first
			cd.valuesToDelete.prev = nil
		} else {
			// Найдём последний p у которого время меньше чем у cd
			p := *first
			for p.valuesToDelete.next != nil {
				if p.valuesToDelete.next.time.After(cd.time) {
					break
				}
				p = p.valuesToDelete.next
			}

			// Вставим cd после p
			cd.valuesToDelete.prev = p
			if p.valuesToDelete.next != nil {
				p.valuesToDelete.next.valuesToDelete.prev = cd
			}

			cd.valuesToDelete.next = p.valuesToDelete.next
			p.valuesToDelete.next = cd
		}
	} else {
		cd.valuesToDelete.prev = nil
		cd.valuesToDelete.next = nil
	}

	if *last == cd.valuesToDelete.prev {
		*last = cd
	}

	if *first == cd.valuesToDelete.next {
		*first = cd
	}
}

type RefCntMemCache[Key comparable, Value any] struct {
	lifetime    time.Duration
	maxElements int

	commands     chan command
	waitStopRcmc chan struct{}

	// Область защищённая мьютексом
	mut            sync.Mutex
	values         map[Key]*cacheDatum[Value]
	valuesToDelete struct {
		first *cacheDatum[Value]
		last  *cacheDatum[Value]
	} // Список элементов которые можно удалять. Упорядочено по time.
	freeFunc func(value *Value)
}

type noCopy struct{}

func (*noCopy) Lock() {}

func (*noCopy) Unlock() {}

// Handle и его методы безопасно использовать только из одной нити.
type Handle[Value any] struct {
	noCopy
	keylessRefCntMemCache keylessRefCntMemCache[Value]
	cacheDatum            *cacheDatum[Value]
	value                 *Value
}

func (handle *Handle[Value]) Get() *Value {
	return handle.value
}

func (handle *Handle[Value]) Put() {
	if handle.keylessRefCntMemCache != nil && handle.cacheDatum != nil {
		handle.keylessRefCntMemCache.put(handle)
	}
}

func (handle *Handle[Value]) Move() (ret Handle[Value]) {
	ret = Handle[Value]{keylessRefCntMemCache: handle.keylessRefCntMemCache, cacheDatum: handle.cacheDatum, value: handle.value}

	handle.cacheDatum = nil
	handle.value = nil
	return
}

func (handle *Handle[Value]) Copy() Handle[Value] {
	if handle.keylessRefCntMemCache != nil && handle.cacheDatum != nil {
		return handle.keylessRefCntMemCache.copy(handle)
	} else {
		return Handle[Value]{}
	}
}

func New[Key comparable, Value any](lifetime time.Duration, maxElements int) *RefCntMemCache[Key, Value] {
	rcmc := &RefCntMemCache[Key, Value]{lifetime: lifetime, maxElements: maxElements}
	rcmc.commands = make(chan command, 1)
	rcmc.waitStopRcmc = make(chan struct{})
	rcmc.values = make(map[Key]*cacheDatum[Value])

	deleteOldValues := func() {
		timer := time.NewTimer(24 * time.Hour)

		for {
			duration := func() (duration time.Duration) {
				rcmc.mut.Lock()
				defer rcmc.mut.Unlock()

				if lifetime > 0 && rcmc.valuesToDelete.first != nil {
					duration = rcmc.valuesToDelete.first.time.Add(lifetime).Sub(time.Now())
					if duration < 0 {
						duration = time.Duration(0)
					}
				} else {
					duration = 24 * time.Hour // Большое время без всякого смысла
				}

				return
			}()

			timer.Stop()
			timer.Reset(duration)

			select {
			case cmd := <-rcmc.commands:
				if cmd == stopProcessDeletedValues {
					rcmc.waitStopRcmc <- struct{}{}
					break
				} else if cmd == listOfDeletedValuesIsChanged {
					// empty
				}
			case <-timer.C:
				func() {
					rcmc.mut.Lock()
					defer rcmc.mut.Unlock()
					rcmc.freeValuesToDelete(true, false)
				}()
			}
		}
	}

	if lifetime > 0 {
		go deleteOldValues()
	}

	return rcmc
}

func (rcmc *RefCntMemCache[Key, Value]) Close() error {
	err := func() error {
		rcmc.mut.Lock()
		defer rcmc.mut.Unlock()

		for _, cacheDatum := range rcmc.values {
			if cacheDatum.counter > 0 {
				return ErrRefIsNotZero
			}
		}

		rcmc.values = make(map[Key]*cacheDatum[Value])

		return nil
	}()
	if err != nil {
		return err
	}

	if rcmc.lifetime > 0 {
		rcmc.commands <- stopProcessDeletedValues
		<-rcmc.waitStopRcmc
	}

	return nil
}

func (rcmc *RefCntMemCache[Key, Value]) SetFree(free func(value *Value)) {
	rcmc.mut.Lock()
	defer rcmc.mut.Unlock()

	rcmc.freeFunc = free
}

// Не thread-safe. Удаляет старые элементы и те, что свехр нормы.
func (rcmc *RefCntMemCache[Key, Value]) freeValuesToDelete(byTime, byMaxElements bool) {
	removeFirst := func(cacheDatum *cacheDatum[Value]) {
		cacheDatum.extractFromList(&rcmc.valuesToDelete.first, &rcmc.valuesToDelete.last)

		cacheDatumValue := cacheDatum.value
		cacheDatum.value = nil

		key := cacheDatum.key.(Key)
		delete(rcmc.values, key)

		if rcmc.freeFunc != nil {
			func() {
				rcmc.mut.Unlock()
				defer rcmc.mut.Lock()
				rcmc.freeFunc(cacheDatumValue)
			}()
		}
	}

	if byTime && rcmc.lifetime > 0 {
		for rcmc.valuesToDelete.first != nil {
			cacheDatum := rcmc.valuesToDelete.first
			if cacheDatum.time.Add(rcmc.lifetime).Before(time.Now()) {
				removeFirst(cacheDatum)
			}
		}
	}

	if byMaxElements && rcmc.maxElements > 0 {
		if len(rcmc.values) > rcmc.maxElements {
			remove := len(rcmc.values) - rcmc.maxElements
			for remove > 0 && rcmc.valuesToDelete.first != nil {
				cacheDatum := rcmc.valuesToDelete.first
				removeFirst(cacheDatum)
				remove--
			}
		}
	}
}

func (rcmc *RefCntMemCache[Key, Value]) put(handle *Handle[Value]) {
	cacheDatum := handle.cacheDatum
	handle.cacheDatum = nil
	handle.value = nil

	if cacheDatum == nil {
		return
	}

	isListChanged := func() bool {
		rcmc.mut.Lock()
		defer rcmc.mut.Unlock()

		if cacheDatum.counter == 0 {
			return false
		}

		cacheDatum.counter--

		if cacheDatum.counter > 0 {
			return false
		}

		cacheDatum.insertToList(&rcmc.valuesToDelete.first, &rcmc.valuesToDelete.last, rcmc.lifetime > 0)

		rcmc.freeValuesToDelete(false, true)

		return true
	}()

	if rcmc.lifetime > 0 && isListChanged && len(rcmc.commands) == 0 {
		rcmc.commands <- listOfDeletedValuesIsChanged
	}

	return
}

func (rcmc *RefCntMemCache[Key, Value]) copy(handle *Handle[Value]) Handle[Value] {
	if handle.cacheDatum == nil {
		return Handle[Value]{}
	}

	rcmc.mut.Lock()
	defer rcmc.mut.Unlock()

	if handle.cacheDatum.counter == 0 {
		panic("counter cannot be 0 at this location")
	}

	handle.cacheDatum.counter++

	return Handle[Value]{noCopy{}, rcmc, handle.cacheDatum, handle.value}
}

func (rcmc *RefCntMemCache[Key, Value]) Get(key Key) (handle Handle[Value]) {
	handle = Handle[Value]{noCopy{}, rcmc, nil, nil}

	isListChanged := func() bool {
		rcmc.mut.Lock()
		defer rcmc.mut.Unlock()

		cacheDatum, exists := rcmc.values[key]
		if !exists {
			return false
		}

		handle.cacheDatum = cacheDatum
		handle.value = cacheDatum.value

		cacheDatum.counter++

		// Обновляем время доступа и убираем из списка на удаление
		cacheDatum.time = time.Now()
		cacheDatum.extractFromList(&rcmc.valuesToDelete.first, &rcmc.valuesToDelete.last)

		return true
	}()

	if rcmc.lifetime > 0 && isListChanged && len(rcmc.commands) == 0 {
		rcmc.commands <- listOfDeletedValuesIsChanged
	}

	return
}

func (rcmc *RefCntMemCache[Key, Value]) Set(key Key, value *Value) (handle Handle[Value], new bool) {
	func() {
		rcmc.mut.Lock()
		defer rcmc.mut.Unlock()

		datum, exists := rcmc.values[key]
		if !exists {
			datum = &cacheDatum[Value]{0, key, listLinks[Value]{}, time.Time{}, value}
			rcmc.values[key] = datum
			new = true
		}

		handle = Handle[Value]{noCopy{}, rcmc, datum, datum.value}
		datum.counter++

		// Обновляем время доступа и убираем из списка на удаление
		datum.time = time.Now()
		datum.extractFromList(&rcmc.valuesToDelete.first, &rcmc.valuesToDelete.last)

		rcmc.freeValuesToDelete(false, true)
	}()

	if rcmc.lifetime > 0 && len(rcmc.commands) == 0 {
		rcmc.commands <- listOfDeletedValuesIsChanged
	}

	return
}

// TODO: надо бы заменить list для valuesToDelete на дерево поиска.
