import xml.etree.ElementTree as ET
import networkx as nx


class OWSInterface:
    def __init__(self, xml_data):
        self.__xml_data__ = xml_data

    def generate_graph(self):
        # Parse the XML
        root = ET.fromstring(self.__xml_data__)

        # Create a directed graph
        G = nx.DiGraph()

        # Add nodes to the graph
        for node in root.find("nodes"):
            node_id = node.get("id")
            node_attr = {
                "name": node.get("name"),
                "position": node.get("position"),
                "project_name": node.get("project_name"),
                "qualified_name": node.get("qualified_name"),
                "title": node.get("title"),
                "version": node.get("version"),
            }
            G.add_node(node_id, **node_attr)

        # Add edges to the graph
        for link in root.find("links"):
            source_node_id = link.get("source_node_id")
            sink_node_id = link.get("sink_node_id")
            link_attr = {
                "enabled": link.get("enabled"),
                "sink_channel": link.get("sink_channel"),
                "source_channel": link.get("source_channel"),
            }
            G.add_edge(source_node_id, sink_node_id, **link_attr)

        # Now you can work with the graph 'G' using networkx functionalities
        return G

    @staticmethod
    def create_mermaid_uml_from_graph(G):
        mermaid_str = "classDiagram\n"

        # Iterate over nodes to define classes
        for node, attr in G.nodes(data=True):
            # Replace this with actual logic to determine the class details from the node attributes
            class_name = attr.get("name", "UnnamedClass")
            mermaid_str += f"    class {class_name} {{\n"
            mermaid_str += f'        {attr.get("title", "Untitled")}\n'
            mermaid_str += "    }\n"

        # Iterate over edges to define relationships
        for source, target, attr in G.edges(data=True):
            source_class_name = G.nodes[source].get("name", "UnnamedClass")
            target_class_name = G.nodes[target].get("name", "UnnamedClass")
            # Assuming all edges are associations for this example
            mermaid_str += f'    {source_class_name} --> {target_class_name} : {attr.get("source_channel", "connects to")}\n'

        return mermaid_str

    @staticmethod
    def find_parallel_steps(G):
        parallel_groups = []

        # Make a copy of the graph to preserve the original
        G_copy = G.copy()

        while G_copy.nodes:
            # Find all nodes with an in-degree of 0
            in_degree_zero_nodes = [
                node for node, degree in G_copy.in_degree() if degree == 0
            ]

            # If there are no nodes with in-degree 0, the graph has a cycle or is fully processed
            if not in_degree_zero_nodes:
                break

            # Group nodes with in-degree 0 as they can be processed in parallel
            parallel_groups.append(in_degree_zero_nodes)

            # Remove nodes with in-degree 0 from the graph
            G_copy.remove_nodes_from(in_degree_zero_nodes)

        return parallel_groups

    @classmethod
    def mermaid_uml_parallel_steps(cls, G):
        groups_of_parallel_steps = cls.find_parallel_steps(G)

        # Base template for Mermaid diagram
        mermaid_diagram = "```mermaid\ngraph TD\n"

        # Keep track of created nodes to avoid duplication in the Mermaid diagram
        created_nodes = set()

        # Add nodes and groups to Mermaid diagram
        for i, group in enumerate(groups_of_parallel_steps, start=1):
            for node_id in group:
                if node_id not in created_nodes:
                    mermaid_diagram += (
                        f"    {node_id}[\"{G.nodes[node_id]['title']}\"]\n"
                    )
                    created_nodes.add(node_id)
            mermaid_diagram += f"    subgraph Parallel Group {i}\n"
            for node_id in group:
                mermaid_diagram += f"        {node_id}\n"
            mermaid_diagram += "    end\n"

        # Close the Mermaid diagram block
        mermaid_diagram += "```\n"

        print(mermaid_diagram)
