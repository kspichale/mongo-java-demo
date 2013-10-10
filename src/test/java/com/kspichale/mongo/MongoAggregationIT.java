package com.kspichale.mongo;

import java.net.UnknownHostException;

import org.junit.Test;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;

public class MongoAggregationIT {

	@Test
	public void testAggregationFramework() throws UnknownHostException {

		MongoClient client = new MongoClient(new ServerAddress("localhost", 37017));
		DB database = client.getDB("demo");
		DBCollection collection = database.getCollection("events");

		BasicDBObject event = new BasicDBObject();
		event.append("title", "Java Forum Stuttgart 2013");
		event.append("participant_number", 1500);
		BasicDBObject venue = new BasicDBObject();
		venue.append("city", "Stuttgart");
		event.append("venue", venue);
		collection.insert(event);

		AggregationOutput output = collection.aggregate(new BasicDBObject("$group", new BasicDBObject("_id",
				"$venue.city").append("count", new BasicDBObject("$sum", "$participant_number"))), new BasicDBObject(
				"$sort", new BasicDBObject("count", 1)));

		for (DBObject doc : output.results()) {
			String city = (String) doc.get("city");
			Integer count = (Integer) doc.get("count");
			System.out.println("In city <" + city + "> are " + count + "visitors.");
		}
	}

}