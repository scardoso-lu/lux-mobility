"""
Walking-distance routing for Luxembourg: address → nearest bus stop.

Core workflow (used by the ETL pipeline):
    G    = load_graph(cache_path)
    maps = build_stop_distance_maps(G, stop_ids, stop_lons, stop_lats)
    dist = maps[stop_id].get(addr_node)   # metres, or None if unreachable

Single-pair helpers and visualisation snippets are at the bottom of this file.
"""
from __future__ import annotations

import csv
from pathlib import Path
from typing import Optional

import networkx as nx
import osmnx as ox

# Luxembourg bounding box with a small margin beyond the border.
_NORTH, _SOUTH, _EAST, _WEST = 50.20, 49.40, 6.60, 5.70

# Dijkstra cutoff: paths longer than this are treated as unreachable.
MAX_WALK_M: float = 3_000.0


def _ox_major() -> int:
    return int(ox.__version__.split(".")[0])


# ── Graph ─────────────────────────────────────────────────────────────────────

def load_graph(cache_path: Path) -> nx.MultiDiGraph:
    """
    Return the Luxembourg pedestrian walking graph.
    Downloads from OpenStreetMap on first call; subsequent calls read the
    cached GraphML file.  Includes highway=footway, pedestrian, path, etc.

    Compatible with osmnx 1.x and 2.x.
    """
    if cache_path.exists():
        print(f"  Loading cached walk graph from {cache_path.name} …")
        return ox.load_graphml(cache_path)

    print(f"  Downloading Luxembourg walking network from OSM (osmnx {ox.__version__}) …")
    if _ox_major() >= 2:
        # osmnx 2.x: bbox = (west, south, east, north)  [GeoJSON left/bottom/right/top]
        G = ox.graph_from_bbox(
            (_WEST, _SOUTH, _EAST, _NORTH),
            network_type="walk",
            simplify=True,
        )
    else:
        # osmnx 1.x: individual keyword arguments
        G = ox.graph_from_bbox(
            north=_NORTH, south=_SOUTH, east=_EAST, west=_WEST,
            network_type="walk",
            simplify=True,
            retain_all=False,
        )
    ox.save_graphml(G, cache_path)
    print(
        f"  Saved {G.number_of_nodes():,} nodes / {G.number_of_edges():,} edges"
        f" → {cache_path.name}"
    )
    return G


# ── Node snapping ─────────────────────────────────────────────────────────────

def snap_nodes(
    G: nx.MultiDiGraph,
    lons: list[float],
    lats: list[float],
) -> list[int]:
    """Vectorised nearest-node lookup.  Returns one node ID per (lon, lat) pair."""
    # nearest_nodes moved to top-level in osmnx 2.x; kept in ox.distance in 1.x.
    fn = getattr(ox, "nearest_nodes", None) or ox.distance.nearest_nodes
    return list(fn(G, X=lons, Y=lats))


# ── Batch routing ─────────────────────────────────────────────────────────────

def build_stop_distance_maps(
    G: nx.MultiDiGraph,
    stop_ids: list[str],
    stop_lons: list[float],
    stop_lats: list[float],
) -> dict[str, dict[int, float]]:
    """
    Run single-source Dijkstra once from every unique stop node.

    Returns
    -------
    {stop_id: {network_node_id: walk_distance_m}}

    Only nodes reachable within MAX_WALK_M are included; missing entries
    indicate that no path shorter than the cutoff exists.
    """
    unique: dict[str, tuple[float, float]] = {}
    for sid, lon, lat in zip(stop_ids, stop_lons, stop_lats):
        unique.setdefault(sid, (lon, lat))

    sids  = list(unique)
    lons_ = [unique[s][0] for s in sids]
    lats_ = [unique[s][1] for s in sids]
    nodes = snap_nodes(G, lons_, lats_)

    print(
        f"  Dijkstra from {len(sids):,} unique stop nodes"
        f" (cutoff {MAX_WALK_M:.0f} m) …"
    )
    maps: dict[str, dict[int, float]] = {}
    for sid, nid in zip(sids, nodes):
        maps[sid] = nx.single_source_dijkstra_path_length(
            G, nid, cutoff=MAX_WALK_M, weight="length"
        )
    return maps


# ── Single-pair helpers ───────────────────────────────────────────────────────

def route_between(
    G: nx.MultiDiGraph,
    orig_lat: float,
    orig_lon: float,
    dest_lat: float,
    dest_lon: float,
) -> tuple[float, list[int]]:
    """
    Shortest walking route between two geographic points.

    Returns
    -------
    (distance_m, [node_id, …])

    Raises nx.NetworkXNoPath if no path exists within the graph.
    """
    fn   = getattr(ox, "nearest_nodes", None) or ox.distance.nearest_nodes
    orig = fn(G, X=orig_lon, Y=orig_lat)
    dest = fn(G, X=dest_lon, Y=dest_lat)
    dist, path = nx.single_source_dijkstra(G, orig, dest, weight="length")
    return dist, path


# ── Visualisation ─────────────────────────────────────────────────────────────

def plot_route_osmnx(
    G: nx.MultiDiGraph,
    orig_lat: float,
    orig_lon: float,
    dest_lat: float,
    dest_lon: float,
) -> None:
    """
    Plot the shortest walking route using osmnx + matplotlib.

    Example
    -------
    from pathlib import Path
    from walking_distance import load_graph, plot_route_osmnx

    G = load_graph(Path("data/processed/luxembourg_walk.graphml"))
    plot_route_osmnx(G, orig_lat=49.6116, orig_lon=6.1319,
                        dest_lat=49.6103, dest_lon=6.1350)
    """
    dist, path = route_between(G, orig_lat, orig_lon, dest_lat, dest_lon)
    print(f"Walking distance: {dist:.0f} m  ({len(path)} nodes)")
    plot_fn = getattr(ox, "plot_graph_route", None) or ox.plot.plot_graph_route
    plot_fn(
        G,
        path,
        route_color="#ef4444",
        route_linewidth=4,
        route_alpha=0.8,
        node_size=0,
        bgcolor="#f0f0f0",
        figsize=(12, 12),
    )


def plot_route_folium(
    G: nx.MultiDiGraph,
    orig_lat: float,
    orig_lon: float,
    dest_lat: float,
    dest_lon: float,
    *,
    out_html: Optional[str] = "route.html",
):
    """
    Build an interactive Folium map with the walking route, origin marker
    (blue house icon) and destination marker (red bus icon).

    Saves to *out_html*; returns the folium.Map object when out_html is None.
    Requires: folium  (pip install folium)

    Example
    -------
    from pathlib import Path
    from walking_distance import load_graph, plot_route_folium

    G = load_graph(Path("data/processed/luxembourg_walk.graphml"))
    plot_route_folium(G, orig_lat=49.6116, orig_lon=6.1319,
                         dest_lat=49.6103, dest_lon=6.1350,
                         out_html="bonnevoie_route.html")
    """
    import folium  # noqa: PLC0415

    dist, path = route_between(G, orig_lat, orig_lon, dest_lat, dest_lon)

    route_coords = [(G.nodes[n]["y"], G.nodes[n]["x"]) for n in path]
    centre = ((orig_lat + dest_lat) / 2, (orig_lon + dest_lon) / 2)

    m = folium.Map(location=centre, zoom_start=17, tiles="OpenStreetMap")

    folium.PolyLine(
        route_coords,
        color="#ef4444",
        weight=5,
        opacity=0.85,
        tooltip=f"Walking route: {dist:.0f} m",
    ).add_to(m)

    folium.Marker(
        [orig_lat, orig_lon],
        tooltip="Address",
        icon=folium.Icon(color="blue", icon="home", prefix="fa"),
    ).add_to(m)

    folium.Marker(
        [dest_lat, dest_lon],
        tooltip=f"Bus stop — {dist:.0f} m walk",
        icon=folium.Icon(color="red", icon="bus", prefix="fa"),
    ).add_to(m)

    if out_html:
        m.save(out_html)
        print(f"Map saved to {out_html}  (walk: {dist:.0f} m)")
        return None
    return m
