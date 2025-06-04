from unittest import TestCase

from bwb_scheduler.ows_to_networkx_graph import OWSInterface


class TestOWSConversion(TestCase):
    def test_load_ows(self):
        with open("tests/sample_ows.xml") as fd:
            xml_data = fd.read()

        graph = OWSInterface(xml_data).generate_graph()
        mermaid_uml = OWSInterface.create_mermaid_uml_from_graph(graph)
        print(mermaid_uml)

        parallel_group = OWSInterface.find_parallel_steps(graph)
        print(parallel_group)

        OWSInterface.mermaid_uml_parallel_steps(graph)

        raise ValueError
