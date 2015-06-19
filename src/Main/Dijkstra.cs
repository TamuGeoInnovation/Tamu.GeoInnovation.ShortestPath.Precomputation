using USC.GISResearchLab.ShortestPath.GraphStructure;

namespace USC.GISResearchLab.ShortestPath.Precomputation
{
	public class Dijkstra
	{
		BinaryHeap Q;
		Graph g;
		Node s;

		public Dijkstra(Graph graph, Node src)
		{
			g = graph;
			s = src;
			s.G = 0;
			Q = new BinaryHeap(g);
			Q.BuildHeap();
		}
		public Node SRC
		{
			get { return s; }
		}

		public void calculateAllPaths(float bufsiz, Graph.MetricType currentMetricType)
		{
			int count = 0;
			Node current, tempNode;
			double alt;
			lock ("graph")
			{
				// g.Refresh();

				while (!Q.isEmpty())
				{
					current = Q.ExtractMin();
					count++;
					if (current.G > bufsiz) break;

					for (int i = 0; i < current.NeighborsCount; i++)
					{
						tempNode = new Node(g.GetNode(current.Neighbors[i].DestinationNodeID));
						alt = current.G + current.Neighbors[i].GetCost(currentMetricType);
						if (alt < tempNode.G) Q.DecreaseKey(tempNode, alt);
					}
				}
			}
		}

		public void findPath(Graph.MetricType currentMetricType)
		{
			Node current, tempNode;
			double alt;
			lock ("graph")
			{
				while (!Q.isEmpty())
				{
					current = Q.ExtractMin();

					for (int i = 0; i < current.NeighborsCount; i++)
					{
						tempNode = new Node(g.GetNode(current.Neighbors[i].DestinationNodeID));
						alt = current.G + current.Neighbors[i].GetCost(currentMetricType);
						if (alt < tempNode.G)
						{
							Q.DecreaseKey(tempNode, alt);
							tempNode.PreviousNode = current;
						}
					}
				}
			}
		}
	}
}