package api

type Point struct {
	Type        string    `json:"type,omitempty"`
	Coordinates []float64 `json:"coordinates,omitempty"`
}

type Polygon struct {
	Type        string        `json:"type,omitempty"`
	Coordinates [][][]float64 `json:"coordinates,omitempty"`
}

type MultiPolygon struct {
	Type        string        `json:"type,omitempty"`
	Coordinates [][][]float64 `json:"coordinates,omitempty"`
}
