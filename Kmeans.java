package part1;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Random;

class Points 
{
	private double x,y;
	private int id;
	public Points(double x, double y,int id)
	{
		this.x= x;
		this.y= y;
		this.id=id;
	}
	public Points(double x, double y)
	{
		this.x= x;
		this.y= y;	
	}
	public double getx() 
	{
		return x;
	}
	public double gety() 
	{
		return y;
	}
	public void setx(double x) 
	{
		this.x = x;
	}
	public void sety(double y) 
	{
		this.y = y;
	}
	public int getId() 
	{
		return id;
	}
	public void setId(int id) 
	{
		this.id = id;
	}
	public double getDistance(Points point)
	{
		return Math.sqrt(Math.pow((this.gety() - point.gety()), 2) + Math.pow((this.getx() - point.getx()), 2));
		//return sum+Math.pow(EuclidianDistance(point.getY().getPoints().getX(),point.getX()),2);
	}
}

class Cluster 
{
	private int id;
	private ArrayList<Points> pointList = new ArrayList<Points>();
	private Points centroid;
	public Cluster(int id, Points centroid)
	{
		this.id =id;
		this.centroid=centroid;
	}
	public ArrayList<Points> getPointList() 
	{
		return pointList;
	}
	public void setPointList(ArrayList<Points> pointList) 
	{
		this.pointList = pointList;
	}
	public Points getCentroid() 
	{
		return centroid;
	}
	public void setCentroid(Points centroid) 
	{
		this.centroid = centroid;
	}
	public int getId() 
	{
		return id;
	}
	public void setId(int id) 
	{
		this.id = id;
	};
	
	public void Centroid()
	{	
		double x=0, y=0;
		int n = pointList.size();
		for(Points point:this.pointList)
		{
			x+=point.getx();
			y+=point.gety();		
		}	
		this.setCentroid(new Points((double)x/n, (double)y/n));
	}
}

public class Kmeans 
{
	public static ArrayList<Points> pointList; 
	public static ArrayList<Cluster> clusterList;
	
	public static void main(String[] args)  
	{	
		if(args.length == 0)
		{
			System.out.println("Input to be given as : <no.ofclusters> <input-file-name> <output-file-name>");
			System.exit(0);
		}
		int k =Integer.parseInt(args[0]);
		String inputFile=args[1];
		String outputFile =args[2];
		int maxRecomp=25;
		pointList = new ArrayList<Points>(); 
		clusterList=new ArrayList<Cluster>();
		Kmeans kmean= new Kmeans();
		kmean.readData(inputFile);
		kmean.initClusters(k);
		kmean.populateClusters();
	    kmean.computeCluster(maxRecomp);
		kmean.printToFile(outputFile);
	    kmean.sse();
	}
	
	public void printToFile(String output)
	{	
		String directory = System.getProperty("user.dir");
		String seperator=System.getProperty("file.separator");
		StringBuilder builder = new StringBuilder(directory);
		builder.append(seperator+"part1"+seperator+output);
		try 
		{
			PrintWriter writer = new PrintWriter(builder.toString(), "UTF-8");	
			for(Cluster cluster: clusterList)
			{
				int pointCount =cluster.getPointList().size();
				writer.print(cluster.getId()+" : ");
				for(Points point: cluster.getPointList())
				{
					if(pointCount!=1)
					{
						writer.print(point.getId()+",");	
					}
					else
					{
						writer.print(point.getId());
					}
					pointCount--;
			    }
				writer.println("");
			}
			writer.close();
		}
		catch (FileNotFoundException e) 
		{
			e.printStackTrace();
		} 
		catch (UnsupportedEncodingException e) 
		{
			e.printStackTrace();
		}
	}
	
	public void sse()
	{	
		double distSq = 0;
		for(Cluster cluster: clusterList)
		{		
			Points centroid = cluster.getCentroid();  
	    	for(Points point: cluster.getPointList())
	    	{		
	    		double dist = point.getDistance(centroid);
	    		distSq+= Math.pow(dist, 2);
	    	}
	    }
		System.out.println("SSE : "+distSq);
	}
	
	public void computeCluster(int maxRecomp)
	{
		boolean change =false;
		int iter= 1;
		do
		{
			for(Cluster cluster: clusterList)
			{
			Points oldCent = cluster.getCentroid();
			cluster.Centroid();
			Points newCent =cluster.getCentroid();
				if(oldCent.getx()!=newCent.getx() || oldCent.gety()!= newCent.gety())
				{
					change= true;
				}
			}
				if(change)
				{
					clearClusterPointList();
					populateClusters();
				}
			iter++;
		}
		while(change && iter<=maxRecomp );
	}
	
	public void clearClusterPointList()
	{	
		for(Cluster cluster: clusterList)
		{
			cluster.getPointList().clear();
		}
	}
	
	public void initClusters(int k)
	{	
		Random rn = new Random();
        int random;
        for(int i=1; i<=k; i++)
        {
        	random = rn.nextInt(pointList.size());
        	clusterList.add(new Cluster(i, pointList.get(random)));
        }
	}
	
	public void populateClusters()
	{
		for(Points point: pointList)
		{
		double dist=Double.MAX_VALUE;
		int clusterId=1;
			for (Cluster cluster : clusterList) 
			{
				double tmp = point.getDistance(cluster.getCentroid());
				if(tmp<dist)
				{
					dist=tmp;
					clusterId=cluster.getId();
				}
				
			}
			clusterList.get(clusterId-1).getPointList().add(point);
		}
	}
	
	public ArrayList<Points> getCentroids()
	{
		ArrayList<Points> centroidList =new ArrayList<Points>();
		for (Cluster cluster : clusterList) 
		{
			centroidList.add(cluster.getCentroid());
		}
		return centroidList;
	}
	
	public void readData(String inputFile)
	{
		String directory = System.getProperty("user.dir");
		String seperator=System.getProperty("file.separator");
		StringBuilder builder = new StringBuilder(directory);
		builder.append(seperator+"part1"+seperator+inputFile);
		String line =null;
		try
		{
			BufferedReader bufferedReader = new BufferedReader(new FileReader(builder.toString()));
	            while((line = bufferedReader.readLine()) != null) 
	            {
	            	String[] rowData = line.split("\t");
	            	if(!rowData[0].equalsIgnoreCase("id"))
	    			{
	            		pointList.add(new Points(Double.parseDouble(rowData[1]),Double.parseDouble(rowData[2]), Integer.parseInt(rowData[0])));
	    			}
	            }  
	            bufferedReader.close();   
		}
		catch(FileNotFoundException ex) 
		{
			 ex.printStackTrace();    
		}
        catch(IOException ex) 
		{
	         ex.printStackTrace();
	    }
	}
}
