package kafka.xchange;

import java.io.FileInputStream;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import static java.util.concurrent.TimeUnit.*;
import org.json.JSONObject;

import java.io.IOException;
import java.lang.RuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
 
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import com.xeiam.xchange.Exchange;
import com.xeiam.xchange.ExchangeFactory;
import com.xeiam.xchange.service.polling.marketdata.PollingMarketDataService;
import com.xeiam.xchange.dto.marketdata.Ticker;
import com.xeiam.xchange.currency.CurrencyPair;
import com.xeiam.xchange.utils.DateUtils;

import kafka.xchange.ExchangeProvider;


class TickerProducerRunnable implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(TickerProducerRunnable.class);
    private PollingMarketDataService marketDataService;
    private String loadedExchangeName;
    private String topicName;
    private Producer<String, String> producer;

    TickerProducerRunnable(PollingMarketDataService marketDataService,
               String loadedExchangeName,
               Producer<String, String> producer) {
        this.marketDataService = marketDataService;
        this.loadedExchangeName = loadedExchangeName;
        this.topicName = "ticks";
        this.producer = producer;
    }
    

    

    public void run() {
        Ticker ticker = null;
        try {
            ticker = this.marketDataService.getTicker(CurrencyPair.BTC_USD);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        String msg = TickProducer.tickerToJSON(ticker).toString();
        logger.debug("Preparing message for topic " +  "-> " + this.loadedExchangeName + ":" + msg);
        KeyedMessage<String, String> data = new KeyedMessage<String, String>(this.topicName, this.loadedExchangeName, msg);
        this.producer.send(data);
    }

    public void close() {
        this.producer.close();
    }
}


public class TickProducer {
	
    private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    static String ExCheck;
    
    public static void main(String[] args) throws IOException {
    	
        Properties props = new Properties();
        props.put("metadata.broker.list", "localhost:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");

        FileInputStream producerConfigFile = new FileInputStream("config/producer.properties");
        props.load(producerConfigFile);
 
        ProducerConfig config = new ProducerConfig(props);

        FileInputStream configFile = new FileInputStream("config/config.properties");
        props.load(configFile);

        String configuredExchangesProp = props.getProperty("exchanges.active");
        List<String> configuredExchanges = Arrays.asList(configuredExchangesProp.split(","));

        Iterator<Exchange> loadedExchangesIterator = ExchangeProvider.getInstance().getExchanges();
        List<Exchange> loadedExchanges = new ArrayList<Exchange>();
        while(loadedExchangesIterator.hasNext()) {
            Exchange loadedExchangeClass = loadedExchangesIterator.next();
            Exchange loadedExchange = ExchangeFactory.INSTANCE.createExchange(loadedExchangeClass.getClass().getName());
            loadedExchanges.add(loadedExchange);
        }

        Iterator<String> ourExchangesCounter = configuredExchanges.iterator(); //iterator for loop determining number of exchanges in input file
        int checkCount = 0; //counter for determining the maximum number of array indices for an input check
        while(ourExchangesCounter.hasNext()){
        	checkCount++;
        	ourExchangesCounter.next();
        }
        
        String[] testVal = new String[checkCount + 1]; 	//string array for storing values checked within file
        Iterator<String> ourExchangesChecker = configuredExchanges.iterator(); 	//iterator for checking exchange names
        int counter = 0;
        while(ourExchangesChecker.hasNext()){	
        	String ourExchangeCheck = ourExchangesChecker.next(); //string for storing current value of name input from file
        	Iterator<Exchange> theirExchangesChecker = loadedExchanges.iterator();
        	while(theirExchangesChecker.hasNext()){
        		Exchange theirExchange = theirExchangesChecker.next();
        		String theirExchangeCheck = theirExchange.getExchangeSpecification().getExchangeName().toLowerCase(); //This is a STRING LIST of THEIR EXCHANGES
        		//below is a debugging output
        		//System.out.println(theirExchangeCheck + ourExchangeCheck);
        		if(theirExchangeCheck.equals(ourExchangeCheck)){
        			testVal[counter] = ourExchangeCheck; //assigning string value of all valid inputs from file
        			//System.out.println(testVal[counter]); //for debugging
        			counter++;  //value to determine if an error may have occurre in input file
        		}
        	}
        }
      	
        ourExchangesChecker = configuredExchanges.iterator(); //iterator for file string values
        if(counter == checkCount){		//if the counter and the checkcount are equal, all strings matched
        	System.out.println("All exchanges valid");  //value to determine if an error may have occurre in input file
        } else{ //if the counter and checkcount are not equal, the invalid string is to be printed. 
        	System.out.println("Invalid exchange found");
        	int i = 0;
        	boolean tester = true;
        	while(ourExchangesChecker.hasNext() && tester == true){
        		ExCheck = ourExchangesChecker.next();
        		//System.out.println(ExCheck + testVal[i]);
        		if(ExCheck != testVal[i]){
        			tester = false;
        			System.out.println(ExCheck); //print invalid exchange
        		}
        		i++;
        		
        	}
        }
        
        //End of config.properties check
        
        loadedExchangesIterator = loadedExchanges.iterator();
        while(loadedExchangesIterator.hasNext()) {
            Exchange loadedExchange = loadedExchangesIterator.next();
            String loadedExchangeName = loadedExchange.getExchangeSpecification().getExchangeName().toLowerCase();
            if (!configuredExchanges.contains(loadedExchangeName)) {
                break;
            }

            PollingMarketDataService marketDataService = loadedExchange.getPollingMarketDataService();

            Producer<String, String> producer = new Producer<String, String>(config);

            TickerProducerRunnable tickerProducer = new TickerProducerRunnable(marketDataService, loadedExchangeName, producer);
            try {
                ScheduledFuture<?> tickerProducerHandler =
                  scheduler.scheduleAtFixedRate(tickerProducer, 0, 10, SECONDS);
            } catch (Exception e) {
                tickerProducer.close();
                throw new RuntimeException(e);
            }
        }
    }

    public static JSONObject tickerToJSON(Ticker ticker) {
        JSONObject json = new JSONObject();

        json.put("pair", ticker.getCurrencyPair());
        json.put("last", ticker.getLast());
        json.put("bid", ticker.getBid());
        json.put("ask", ticker.getAsk());
        json.put("high", ticker.getHigh());
        json.put("low", ticker.getLow());
        json.put("avg", ticker.getVwap());
        json.put("volume", ticker.getVolume());
        json.put("timestamp", DateUtils.toMillisNullSafe(ticker.getTimestamp()));

        return json;
    }
}
