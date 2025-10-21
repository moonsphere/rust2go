package mem_ring

// deque implements the minimal operations we need from github.com/edwingeng/deque/v2

type Deque[T any] struct {
	items []T
}

func NewDeque[T any]() *Deque[T] {
	return &Deque[T]{items: make([]T, 0)}
}

func (d *Deque[T]) PushBack(item T) {
	d.items = append(d.items, item)
}

func (d *Deque[T]) PushFront(item T) {
	d.items = append(d.items, item)
	copy(d.items[1:], d.items[:len(d.items)-1])
	d.items[0] = item
}

func (d *Deque[T]) TryPopFront() (T, bool) {
	if len(d.items) == 0 {
		var zero T
		return zero, false
	}
	item := d.items[0]
	// Avoid holding reference to item after removal
	copy(d.items[0:], d.items[1:])
	var zero T
	d.items[len(d.items)-1] = zero
	d.items = d.items[:len(d.items)-1]
	return item, true
}

func (d *Deque[T]) IsEmpty() bool {
	return len(d.items) == 0
}
