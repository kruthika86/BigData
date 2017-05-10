package part2;

import java.io.BufferedInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

public class TweetsClustering {
	public static void main(String[] args) {
		String tweets = null;
		try {
		    BufferedInputStream inputStream = (BufferedInputStream) TweetsClustering.class.getResourceAsStream("\\Tweets.json");
		    tweets = IOUtils.toString(inputStream);
		    inputStream.close();
		} catch (FileNotFoundException e) {
		    e.printStackTrace();
		} catch (IOException e) {
		    e.printStackTrace();
		}
		LinkedHashMap<Long,String> tweet = new LinkedHashMap<Long,String>();
		int start = 0;
		int end = 0;
		int tweetCount = StringUtils.countMatches(tweets, "{")/2;
		//System.out.println("Number of Tweets : "+tweetCount);
		for (int i = 1;i<=tweetCount;i++){
			if(i==1){
				start = tweets.indexOf("{");
				end = tweets.indexOf("}")+2;
				String value = tweets.substring(start,end).toLowerCase().replaceAll("\"", "");
				long id = Long.parseLong(value.substring(value.indexOf(", id: ")+6,value.indexOf(", iso_language")));
				tweet.put(id, value);
				//System.out.println(id+" "+value);
			}else{
				tweets = tweets.substring(end+1, tweets.length());	
				start = tweets.indexOf("{");
				end = tweets.indexOf("}")+2;
				String value = tweets.substring(start,end).toLowerCase().replaceAll("\"", "");
				long id = Long.parseLong(value.substring(value.indexOf(", id: ")+6,value.indexOf(", iso_language")));
				tweet.put(id, value);
				//System.out.println(id+" "+value);
			}
		}
		Set<Long> keys = new LinkedHashSet<Long>();
		keys = tweet.keySet();
		LinkedList<Long> ids = new LinkedList<Long>();
		for (Long id:keys){
			ids.add(id);
		}
		LinkedHashMap<String,String> result = new LinkedHashMap<String,String>();
		for(int i=0;i<tweetCount-1;i++){
			for(int j=i+1;j<tweetCount;j++){
				//System.out.println("I : "+i);
				//System.out.println("J : "+j);
				String tweet1 = tweet.get(ids.get(i)).toString();
			    String tweet2 = tweet.get(ids.get(j)).toString();
			    tweet1 = tweet1.substring(7, tweet1.indexOf(", profile_image_url"));
			    tweet2 = tweet2.substring(7, tweet2.indexOf(", profile_image_url"));
			    //System.out.println("Tweet# : "+tweet1Id+" Tweet : "+tweet1);
			    //System.out.println("Tweet# : "+tweet2Id+" Tweet : "+tweet2);
			    //System.out.println("-------------------------------------");
			    String tweet1Array[];
			    String tweet2Array[];
			    tweet1Array = tweet1.split(" ");
			    tweet2Array = tweet2.split(" ");
			    LinkedHashSet<String> jaccardUnionSet = new LinkedHashSet<String>();
			    LinkedHashSet<String> jaccardIntersectSet = new LinkedHashSet<String>();
			    LinkedList<String> tweet1List = new LinkedList<String>();
			    LinkedList<String> tweet2List = new LinkedList<String>();
			    
			    for(int k = 0; k < tweet1Array.length; k++)
			    {
			    	jaccardUnionSet.add(tweet1Array[k].toString());
			    }
			    
			    for(int l = 0; l < tweet2Array.length; l++)
			    {
			    	jaccardUnionSet.add(tweet2Array[l].toString());
			    }
			    float jaccardUnionCount = jaccardUnionSet.size();
			    for(int m = 0; m < tweet1Array.length; m++)
			    {
			    	tweet1List.add(tweet1Array[m].toString());
			    }
			    for(int n = 0; n < tweet2Array.length; n++)
			    {
			    	tweet2List.add(tweet2Array[n].toString());
			    }
			    tweet1List.retainAll(tweet2List);
			    for(String o :tweet1List){
			    	jaccardIntersectSet.add(o);
			    }
			    float jaccardIntersectCount = jaccardIntersectSet.size();
			    float jaccardDistance = (float)1-(jaccardIntersectCount/jaccardUnionCount);
			    //System.out.println("Tweet : "+tweet1);
			    //System.out.println("Tweet : "+tweet2);
			    //System.out.println("Comparing : "+ids.get(i)+" and "+ids.get(j)+" | UnionCount : "+jaccardUnionCount+" IntersectCount : "+jaccardIntersectCount+" Jaccard Distance : "+jaccardDistance);
			    //System.out.println("-------------------------------------");
			    if(jaccardDistance==0.0){
			    	result.put(""+ids.get(i)+" and "+ids.get(j),"SAME");	
			    }else if(jaccardDistance==1.0){
			    	result.put(""+ids.get(i)+" and "+ids.get(j),"DIFFERENT");
			    }else if(jaccardDistance>0.0 && jaccardDistance<0.5){
			    	result.put(""+ids.get(i)+" and "+ids.get(j),"SIMILAR");
			    }else if(jaccardDistance>=0.5 && jaccardDistance<1.0){
			    	result.put(""+ids.get(i)+" and "+ids.get(j),"NOT SIMILAR");
			    }
			}//if(i==1)break;
		}
		for (Map.Entry<String, String> cluster : result.entrySet()){
			//if(cluster.getValue().toString().equalsIgnoreCase("SAME"))
			System.out.println("The Tweets "+cluster.getKey()+" are "+cluster.getValue());
		}
	}
}