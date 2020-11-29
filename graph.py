import sqlite3
from collections import namedtuple

from graph_tool import Graph


class MentionEdge(
    namedtuple(
        "MentionEdge", ["from_user_id", "to_user_id", "from_name", "to_name", "weight"]
    )
):
    __slots__ = ()

    def __str__(self):
        return f"({self.from_name} -> {self.to_name}: {self.weight})"


def main():
    conn = sqlite3.connect("file:./discord_archive.sqlite3?mode=ro", uri=True)
    graph = Graph(directed=True)

    from_name = graph.new_edge_property("string")
    to_name = graph.new_edge_property("string")
    weight = graph.new_edge_property("int64_t")
    from_id = graph.new_edge_property("int64_t")
    to_id = graph.new_edge_property("int64_t")

    name = graph.new_vertex_property("string")

    graph.edge_properties["from_name"] = from_name
    graph.edge_properties["to_name"] = to_name 
    graph.edge_properties["weight"] = weight 

    graph.vertex_properties["name"] = name

    edges = (
        MentionEdge._make(row)
        for row in conn.execute(
            """
        WITH raw_mentions AS (
            SELECT
                from_user_id,
                to_user_id,
                count(1) AS weight,
                rowid
            FROM mentions
            WHERE 
                -- Nadeko
                from_user_id != 632001928263630871
                AND to_user_id != 632001928263630871
                -- Dyno
                AND from_user_id != 155149108183695360
                AND to_user_id != 155149108183695360
                -- Remove self mentions
                AND from_user_id != to_user_id
            GROUP BY 1, 2
            ORDER BY weight DESC
            LIMIT 4000
        )
        SELECT
            from_user_id,
            to_user_id,
            coalesce(u1.display_name || '#' || u1.discriminator, 'Unknown User#' || rowid),
            coalesce(u2.display_name || '#' || u2.discriminator, 'Unknown User#' || rowid),
            weight
        FROM raw_mentions
        LEFT JOIN users u1 ON (from_user_id = u1.user_id)
        LEFT JOIN users u2 ON (to_user_id = u2.user_id);
    """
        )
    )

    edge_props = [from_name, to_name, weight]
    graph.add_edge_list(edges, hashed=True, hash_type="int64_t", eprops=edge_props)


    # This is kinda slow but whatever.
    for edge in graph.edges():
        source, target = edge.source(), edge.target()
        graph.vertex_properties.name[source] = graph.edge_properties.from_name[edge]
        graph.vertex_properties.name[target] = graph.edge_properties.to_name[edge]

    print(graph)
    graph.save("mentions.graphml", fmt="graphml")


if __name__ == "__main__":
    main()