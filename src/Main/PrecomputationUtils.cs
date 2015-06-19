using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using USC.GISResearchLab.ShortestPath.GraphStructure;

using USC.GISResearchLab.Common.Geographics.DistanceFunctions;
using USC.GISResearchLab.Common.Geographics.Units;

namespace USC.GISResearchLab.ShortestPath.Precomputation
{
	public class PrecomputationUtils
	{
		public static void Precompute(List<Node> lms, Graph g, string connectionString)
		{
			Dijkstra dj;
			lock ("graph")
			{
				foreach (Node src in lms)
				{
					dj = new Dijkstra(g, src);
					dj.calculateAllPaths(10, Graph.MetricType.Distance);
					PrecomputationUtils.Save2DB(src, g, Graph.MetricType.Distance, connectionString, "AvailableRoadNetworkData", g.RoadNetworkDBName);
				}
			}
		}
		public static void Save2DB(Node source, Graph graph, Graph.MetricType metricType, string connectionString, string masterTableName, string roadNetworkBaseName)
		{
			bool IsCostDist = (metricType == Graph.MetricType.Distance);
			string InsertCommandText = "";
			var con = new SqlConnection(connectionString);
			SqlCommand com = null;
			var tbl = new DataTable();
			try
			{
				// Validation
				con.Open();
				com = con.CreateCommand();
				com.CommandTimeout = 300;
				com.CommandText = "select count(*) from " + masterTableName + " where associatedTableName = '" + roadNetworkBaseName + "'";
				int temp = (int)(com.ExecuteScalar());
				if (temp != 1) throw new Exception("RoadNetwork BaseName is not registered in the Master Table.");

				// create precomp table if not exsist
				if (con.State != ConnectionState.Open) con.Open();
				com.CommandText = "IF OBJECT_ID('" + roadNetworkBaseName + "_PreComp', 'U') IS NULL " +
				  "create TABLE " + roadNetworkBaseName + "_PreComp (" +
				  "[srcLat] [double] NOT NULL,[srcLong] [double] NOT NULL," +
				  "[destLat] [double] NOT NULL,[destLong] [double] NOT NULL," +
				  "[cost] [double] NOT NULL,[IsCostDist] [bit] NOT NULL " +
				  "CONSTRAINT [PK_" + roadNetworkBaseName + "_PreComp_1] PRIMARY KEY CLUSTERED " +
				  "([srcLat] ASC,[srcLong] ASC,[destLat] ASC,[destLong] ASC,[IsCostDist] ASC)) ON [PRIMARY]";
				com.ExecuteNonQuery();

				// Check for specific landmark
				com.CommandText = "select count(*) from " + roadNetworkBaseName + "_PreComp where (srclat = '" + source.Latitude + "') and (srclong = '" + source.Longitude + "') and ([IsCostDist] = '" + IsCostDist + "')";
				if ((int)(com.ExecuteScalar()) != 0) throw new Exception("The Precomputation table already contains data from this source and metricType.");

				// Start inserting
				InsertCommandText = "insert into " + roadNetworkBaseName + "_PreComp VALUES (";
				Node g = null;
				foreach (var a in graph.NodeCollection)
				{
					g = new Node(a);
					if (g.G > 0)
					{
						com.CommandText = InsertCommandText + source.Latitude + "," + source.Longitude + "," + g.Latitude + "," + g.Longitude + "," + g.G + ",'" + IsCostDist + "')";
						com.ExecuteNonQuery();
					}
				}
			}
			catch (Exception e)
			{
				throw new Exception("Something happend during saving the graph in precomputation database table.", e);
			}
		}
		public static int DeleteFromDBBySourceNode(Node source, Graph.MetricType metricType, string connectionString, string masterTableName, string roadNetworkBaseName)
		{
			bool IsCostDist = (metricType == Graph.MetricType.Distance);
			var con = new SqlConnection(connectionString);
			SqlCommand com = null;
			try
			{
				// Validation
				con.Open();
				com = con.CreateCommand();
				com.CommandTimeout = 300;
				com.CommandText = "select count(*) from " + masterTableName + " where associatedTableName = '" + roadNetworkBaseName + "'";
				if ((int)(com.ExecuteScalar()) != 1) throw new Exception("RoadNetwork BaseName is not registered in the Master Table.");

				com.CommandText = "select count(*) from " + roadNetworkBaseName + "_PreComp where (srclat = '" + source.Latitude + "') and (srclong = '" + source.Longitude + "') and ([IsCostDist] = " + IsCostDist + ")";
				com.ExecuteNonQuery();

				// deletation
				com.CommandText = "delete from " + roadNetworkBaseName + "_PreComp where (srclat = '" + source.Latitude + "') and (srclong = '" + source.Longitude + "') and ([IsCostDist] = " + IsCostDist + ")";
				return com.ExecuteNonQuery();
			}
			catch (Exception e)
			{
				throw new Exception("Something happend during removing a source node from precomputation database table.", e);
			}
		}
		public static List<Node> ListLandmarks(double TopRightLat, double TopRightLong, double BottomLeftLat, double BottomLeftLong
														  , string connectionString, string roadNetworkBaseName)
		{
			List<Node> ret = null;
			string cmdStr = "SELECT srcLat,srcLong FROM " + roadNetworkBaseName + "_PreComp" +
								  "WHERE (srcLat <= " + TopRightLat + ") and (scrLat >= " + BottomLeftLat + ") and " +
								  "(srcLong <= " + TopRightLong + ") and (srcLong >= " + BottomLeftLong + ") GROUP BY srcLat,srcLong";
			var adpt = new SqlDataAdapter(cmdStr, connectionString);
			var tbl = new DataTable();
			try
			{
				// Validation is not recommended because this method will be called at the begining of each search
				adpt.Fill(tbl);
				ret = new List<Node>(tbl.Rows.Count);
				foreach (DataRow r in tbl.Rows) ret.Add(new Node((double)(r["srcLong"]), (double)(r["srcLat"])));
			}
			catch (Exception e)
			{
				throw new Exception("Something happend during listing landmarks in precomputation database table.", e);
			}
			return ret;
		}
		public static Hashtable ReadNodesToHashTable(string connectionString, string roadNetworkBaseName, Graph.MetricType metricType)
		{
			Hashtable myset = null;
			Hashtable lands = null;
			Node tempNode = null;
			bool IsCostDist = (metricType == Graph.MetricType.Distance);
			string cmdStr = "if (select object_id('" + roadNetworkBaseName + "_PreComp','U')) <> '' SELECT * FROM " + roadNetworkBaseName + "_PreComp" +
								  " WHERE IsCostDist = '" + IsCostDist + "'"; //  ORDER BY destLong,destLat
			var adpt = new SqlDataAdapter(cmdStr, connectionString);
			var tbl = new DataTable();
			try
			{
				adpt.Fill(tbl);
				myset = new Hashtable(tbl.Rows.Count / 1000);
				foreach (DataRow r in tbl.Rows)
				{
					tempNode = new Node(double.Parse(r["destLong"].ToString()), double.Parse(r["destLat"].ToString()));

					// if not in myset add it with the current landmark if it esxist, find it and add the landmark to its lands
					if (myset.Contains(tempNode))
					{
						lands = (Hashtable)(myset[tempNode]);
						lands.Add(new Node(double.Parse(r["srcLong"].ToString()), double.Parse(r["srcLat"].ToString())), double.Parse(r["cost"].ToString()));
					}
					else
					{
						lands = new Hashtable();
						lands.Add(new Node(double.Parse(r["srcLong"].ToString()), double.Parse(r["srcLat"].ToString())), double.Parse(r["cost"].ToString()));
						myset.Add(tempNode, lands);
					}
				}
			}
			catch (Exception e)
			{
				throw new Exception("Something happend during reading precomputation database.", e);
			}
			return myset;
		}
		public static List<Node> ComputeLandMarks(Graph g, int apartFromDist)
		{
			List<Node> lms = new List<Node>();

			foreach (Node candidate in g.NodeCollection)
			{
				if (lms.Count == 0)//just insert the first node
					lms.Add(candidate);
				int flagCount = 0;
				foreach (Node existingLM in lms)
				{
					if (GreatCircleDistance.LinearDistance2(existingLM.Latitude, existingLM.Longitude, candidate.Latitude, candidate.Longitude, LinearUnitTypes.Miles) > apartFromDist)
						flagCount++;
					else
						break;
				}
				if (flagCount == lms.Count)
					lms.Add(candidate);
			}
			return lms;
		}
	}
}