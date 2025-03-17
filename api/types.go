package api

type Point struct {
	Type        string    `json:"type,omitempty"`
	Coordinates []float64 `json:"coordinates,omitempty"`
}

type Polygon struct {
	Type        string        `json:"type,omitempty"`
	Coordinates [][][]float64 `json:"coordinates,omitempty"`
}

type MultiPolygon = Polygon

func NewPolygon(coordinates [][]float64) *Polygon {
	polygon := &Polygon{
		Type:        "Polygon",
		Coordinates: [][][]float64{coordinates},
	}
	return polygon
}

func NewMultiPolygon(coordinates [][][]float64) *MultiPolygon {
	multiPolygon := &MultiPolygon{
		Type:        "MultiPolygon",
		Coordinates: coordinates,
	}
	return multiPolygon
}
