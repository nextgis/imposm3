package writer

import (
	"sync"

	osm "github.com/omniscale/go-osm"
	"github.com/nextgis/imposm3/cache"
	"github.com/nextgis/imposm3/database"
	"github.com/nextgis/imposm3/expire"
	geomp "github.com/nextgis/imposm3/geom"
	"github.com/nextgis/imposm3/geom/geos"
	"github.com/nextgis/imposm3/log"
	"github.com/nextgis/imposm3/mapping"
	"github.com/nextgis/imposm3/stats"
)

type WayWriter struct {
	OsmElemWriter
	singleIDSpace  bool
	ways           chan *osm.Way
	lineMatcher    mapping.WayMatcher
	polygonMatcher mapping.WayMatcher
	maxGap         float64
}

func NewWayWriter(
	osmCache *cache.OSMCache,
	diffCache *cache.DiffCache,
	singleIDSpace bool,
	ways chan *osm.Way,
	inserter database.Inserter,
	progress *stats.Statistics,
	polygonMatcher mapping.WayMatcher,
	lineMatcher mapping.WayMatcher,
	srid int,
) *OsmElemWriter {
	maxGap := 1e-1 // 0.1m
	if srid == 4326 {
		maxGap = 1e-6 // ~0.1m
	}
	ww := WayWriter{
		OsmElemWriter: OsmElemWriter{
			osmCache:  osmCache,
			diffCache: diffCache,
			progress:  progress,
			wg:        &sync.WaitGroup{},
			inserter:  inserter,
			srid:      srid,
		},
		singleIDSpace:  singleIDSpace,
		lineMatcher:    lineMatcher,
		polygonMatcher: polygonMatcher,
		ways:           ways,
		maxGap:         maxGap,
	}
	ww.OsmElemWriter.writer = &ww
	return &ww.OsmElemWriter
}

func (ww *WayWriter) wayID(id int64) int64 {
	if !ww.singleIDSpace {
		return id
	}
	return -id
}

func (ww *WayWriter) loop() {
	geos := geos.NewGeos()
	geos.SetHandleSrid(ww.srid)
	defer geos.Finish()
	for w := range ww.ways {
		ww.progress.AddWays(1)
		if len(w.Tags) == 0 {
			continue
		}

		filled := false
		// fill loads all coords. call only if we have a match
		fill := func(w *osm.Way) bool {
			if filled {
				return true
			}
			err := ww.osmCache.Coords.FillWay(w)
			if err != nil {
				return false
			}
			ww.NodesToSrid(w.Nodes)
			filled = true
			return true
		}

		w.ID = ww.wayID(w.ID)

		var err error
		inserted := false
		insertedPolygon := false
		if matches := ww.lineMatcher.MatchWay(w); len(matches) > 0 {
			if !fill(w) {
				continue
			}
			err, inserted = ww.buildAndInsert(geos, w, matches, false)
			if err != nil {
				if errl, ok := err.(ErrorLevel); !ok || errl.Level() > 0 {
					log.Println("[warn]: ", err)
				}
				continue
			}
		}
		if matches := ww.polygonMatcher.MatchWay(w); len(matches) > 0 {
			if !fill(w) {
				continue
			}
			if w.IsClosed() {
				err, insertedPolygon = ww.buildAndInsert(geos, w, matches, true)
				if err != nil {
					if errl, ok := err.(ErrorLevel); !ok || errl.Level() > 0 {
						log.Println("[warn]: ", err)
					}
					continue
				}
			}
		}

		if (inserted || insertedPolygon) && ww.expireor != nil {
			expire.ExpireProjectedNodes(ww.expireor, w.Nodes, ww.srid, insertedPolygon)
		}
		if (inserted || insertedPolygon) && ww.diffCache != nil {
			ww.diffCache.Coords.AddFromWay(w)
		}
	}
	ww.wg.Done()
}

func (ww *WayWriter) buildAndInsert(
	g *geos.Geos,
	w *osm.Way,
	matches []mapping.Match,
	isPolygon bool,
) (error, bool) {

	// make copy to avoid interference with polygon/linestring matches
	way := osm.Way(*w)

	// Shortcut for non-clipped LineStrings:
	// We don't need any function from GEOS, so we can directly create the WKB hex string.
	if ww.limiter == nil && !isPolygon {
		wkb, err := geomp.NodesAsEWKBHexLineString(w.Nodes, ww.srid)
		if err != nil {
			return err, false
		}
		geom := geomp.Geometry{Wkb: wkb}
		if err := ww.inserter.InsertLineString(w.Element, geom, matches); err != nil {
			return err, false
		}
		return nil, true
	}
	// TODO: We could also apply this shortcut for simple Polygons that we we don't
	// need to make valid (e.g. no holes, only 4 points).
	// However, this does not work with (Webmerc)Area columns, unless we have
	// a non-GEOS implementation.
	// if ww.limiter == nil && isPolygon && len(w.Nodes) <= 5 {
	// 	geom := geomp.Geometry{Wkb: geomp.WayAsEWKBHexPolygon(w.Nodes, ww.srid)}
	// 	if err := ww.inserter.InsertPolygon(w.Element, geom, matches); err != nil {
	// 		return err, false
	// 	}
	// 	return nil, true
	// }

	var err error
	var geosgeom *geos.Geom

	if isPolygon {
		geosgeom, err = geomp.Polygon(g, way.Nodes)
		if err == nil {
			if g.NumCoordinates(geosgeom) > 5 {
				// only check for valididty for non-simple geometries
				geosgeom, err = g.MakeValid(geosgeom)
			}
		}
	} else {
		geosgeom, err = geomp.LineString(g, way.Nodes)
	}
	if err != nil {
		return err, false
	}

	geom, err := geomp.AsGeomElement(g, geosgeom)
	if err != nil {
		return err, false
	}

	inserted := true
	if ww.limiter != nil {
		parts, err := ww.limiter.Clip(geom.Geom)
		if err != nil {
			return err, false
		}
		if len(parts) == 0 {
			// outside of limitto
			inserted = false
		}
		for _, p := range parts {
			way := osm.Way(*w)
			geom = geomp.Geometry{Geom: p, Wkb: g.AsEwkbHex(p)}
			if isPolygon {
				if err := ww.inserter.InsertPolygon(way.Element, geom, matches); err != nil {
					return err, false
				}
			} else {
				if err := ww.inserter.InsertLineString(way.Element, geom, matches); err != nil {
					return err, false
				}
			}
		}
	} else {
		if isPolygon {
			if err := ww.inserter.InsertPolygon(way.Element, geom, matches); err != nil {
				return err, false
			}
		} else {
			if err := ww.inserter.InsertLineString(way.Element, geom, matches); err != nil {
				return err, false
			}
		}
	}
	return nil, inserted
}
