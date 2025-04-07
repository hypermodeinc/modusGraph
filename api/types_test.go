package api

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewPolygon(t *testing.T) {
	coordinates := [][]float64{
		{-122.083506, 37.4259518}, // Northwest
		{-122.081506, 37.4259518}, // Northeast
		{-122.081506, 37.4239518}, // Southeast
		{-122.083506, 37.4239518}, // Southwest
		{-122.083506, 37.4259518}, // Close the polygon
	}

	polygon := NewPolygon(coordinates)
	require.NotNil(t, polygon)
	require.Len(t, polygon.Coordinates, 1)
	require.Equal(t, coordinates, polygon.Coordinates[0])
}

func TestNewMultiPolygon(t *testing.T) {
	coordinates := [][][]float64{
		{
			{-122.083506, 37.4259518},
			{-122.081506, 37.4259518},
			{-122.081506, 37.4239518},
			{-122.083506, 37.4239518},
			{-122.083506, 37.4259518},
		},
		{
			{-122.073506, 37.4359518},
			{-122.071506, 37.4359518},
			{-122.071506, 37.4339518},
			{-122.073506, 37.4339518},
			{-122.073506, 37.4359518},
		},
	}

	multiPolygon := NewMultiPolygon(coordinates)
	require.NotNil(t, multiPolygon)
	require.Equal(t, "MultiPolygon", multiPolygon.Type)
	require.Equal(t, coordinates, multiPolygon.Coordinates)
}
