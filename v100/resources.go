package v100

type Resources struct {
	values map[string]float64
}

func NewResources() Resources {
	return Resources{values: make(map[string]float64)}
}

func (r *Resources) Set(name string, value float64) {
	if r.values == nil {
		r.values = make(map[string]float64)
	}
	r.values[name] = value
}

func (r *Resources) Sub(other Resources) *Resources {
	result := NewResources()
	for k, v := range r.values {
		result.values[k] = v
	}
	for k, v := range other.values {
		result.values[k] -= v
	}
	return &result
}

func (r *Resources) Add(other Resources) *Resources {
	result := NewResources()
	for k, v := range r.values {
		result.values[k] = v
	}
	for k, v := range other.values {
		result.values[k] += v
	}
	return &result
}

func (r *Resources) IsValid() bool {
	for _, v := range r.values {
		if v < 0 {
			return false
		}
	}
	return true
}
