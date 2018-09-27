import java.net.*;
import java.io.*;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import java.util.regex.*;

public class WikiCFPScraperTemplate {
	public static int DELAY = 7;
	public static void main(String[] args) {
	
		try {
			
			
			String category[] = {"data mining", "databases", "machine learning", "artificial intelligence"};
			//String category = "databases";
			//String category = "machine learning";
			//String category = "artificial intelligence";
			
	        int numOfPages = 20;
	        
	        //create the output file
	        File file = new File("wikicfp_crawl.txt");
	        file.createNewFile();
	        FileWriter writer = new FileWriter(file); 
	       
	        /* Write columns' headers to the target file */
	        writer.write("Conference's Acronym with year\t");
	        writer.write("Conference's Acronym\t");
	        writer.write("Year\t");
	        writer.write("Conference's Name\t");
	        writer.write("Location\n");
	        
	        //now start crawling the all 'numOfPages' pages
	        for (int categoryIndex = 0; categoryIndex < category.length; categoryIndex++) {
	        	for(int i = 1;i<=numOfPages;i++) {
		        	//Create the initial request to read the first page 
					//and get the number of total results
		        	String linkToScrape = "http://www.wikicfp.com/cfp/call?conference="+
		        				      URLEncoder.encode(category[categoryIndex], "UTF-8") +"&page=" + i;
		        	String content = getPageFromUrl(linkToScrape);
		        	
		        	Document doc = Jsoup.parse(content);
		        	
		        	/* Navigate the content section */
		        	Element cotentSec = doc.select("div.contsec").first();
		        	
		        	/* Select the first table, which contains necessary info of conferences */
		        	Element table = cotentSec.select("table").get(0); 
		        	Elements rows = table.select("tr");

		        	for (int j = 0; j < rows.size(); j++) {
		        	    Element row = rows.get(j);
		        	    Elements cols = row.select("td");

		        	    /* Search for column containing the key word "Event" */	
		        	    if (cols.get(0).text().contains("Event")) {
		        	        /* Target content identified */
		        	    	
		        	    	/* The first table contains all info of conferences */
		        	    	Element tTable = cols.get(0).select("table").first();
		        	    	Elements tRows = tTable.select("tr");
		        	    	int numRows = tRows.size();
		        	    	
		        	    	/* Skip the first row, which contains columns' titles */
		        	    	int k = 1;
		        	    	while (k < numRows) {
		        	    		/* Each conference record has two consecutive rows:
			        	    	 * First row: conference acronym + conference name
			        	    	 * Second row: when + where + deadline */
		        	    		Element firstRow, secondRow;
		        	    		
		        	    		firstRow = tRows.get(k++);
		        	    		if (k < numRows) {
		        	    			Elements firstRowCols;
		        	    			firstRowCols = firstRow.select("td");
		        	    			
		        	    			if (firstRowCols.size() == 1) {
		        	    				/* Skip this row since it does not contain necessary info */
		        	    				System.out.println(firstRowCols.get(0).text());
		        	    			}
		        	    			else {
		        	    				Elements secondRowCols;
		        	    				
		        	    				/* We want to search for pattern of "Conference's acronym + space or ' + conference's year" */
		        	    				Pattern p = Pattern.compile("(.+)[\\s\\'](\\d+)");
		        	    				Matcher m;
		        	    				
		        	    				/* Get the second row of a record */
			        	    			secondRow = tRows.get(k++);
			        	    			secondRowCols = secondRow.select("td");
			        	    			
			        	    			/* Write conference's acronym with year to the target file */
		        	    				writer.write(firstRowCols.get(0).text() + "\t");
		        	    				
			        	    			/* Get conference's acronym and year */
			        	    			m = p.matcher(firstRowCols.get(0).text());
			        	    			//m = p.matcher("ICNP2016");
			        	    			if (m.find()) {
			        	    				/* Write the conference's acronym to the target file */
			        	    				writer.write(m.group(1) + "\t");
			        	    				//System.out.println(m.group(1));
			        	    				
			        	    				/* Write the conference's year to the target file */
			        	    				writer.write(m.group(2) + "\t");
			        	    				//System.out.println(m.group(2));
			        	    			}
			        	    			else {
			        	    				/* We need a new pattern for this special case */
			        	    				Pattern p1 = Pattern.compile("(\\D+)(\\d+)");
			        	    				Matcher m1 = p1.matcher(firstRowCols.get(0).text());
			        	    				
			        	    				if (m1.find()) {
			        	    					/* Write the conference's acronym to the target file */
				        	    				writer.write(m1.group(1) + "\t");
				        	    				//System.out.println(m1.group(1));
				        	    				
				        	    				/* Write the conference's year to the target file */
				        	    				writer.write(m1.group(2) + "\t");
				        	    				//System.out.println(m1.group(2));
			        	    				}
			        	    				else {
			        	    					/* Write whatever we have for acronym to the target file */
				        	    				writer.write(firstRowCols.get(0).text() + "\t");
				        	    				
				        	    				/* Year will be NA */
				        	    				writer.write("NA" + "\t");
				        	    				System.out.println("Year is NA");
			        	    				}
			        	    			}
			        	    			
			        	    			/* Write the conference's name to the target file */
			        	    			writer.write(firstRowCols.get(1).text() + "\t");
			        	    			
			        	    			/* Write the conference's location to the target file */
			        	    			writer.write(secondRowCols.get(1).text() + "\n");
		        	    			}
		        	    		}
		        	    		else {
		        	    			/* Invalid record */
		        	    			System.out.println("Error on page #" + i + ", k = " + k);
		        	    			break;
		        	    		}
		        	    	}
		        	    	
		        	    	/* Already got all necessary info */
		        	        break;
		        	    }
		        	}
		        	
		        	//System.out.println(content);
		        		        	
		        	//IMPORTANT! Do not change the following:
		        	Thread.sleep(DELAY*1000); //rate-limit the queries
		        }
	        }

        writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}
	
	/**
	 * Given a string URL returns a string with the page contents
	 * Adapted from example in 
	 * http://docs.oracle.com/javase/tutorial/networking/urls/readingWriting.html
	 * @param link
	 * @return
	 * @throws IOException
	 */
	public static String getPageFromUrl(String link) throws IOException {
		URL thePage = new URL(link);
        URLConnection yc = thePage.openConnection();
        BufferedReader in = new BufferedReader(new InputStreamReader(
                                    yc.getInputStream()));
        String inputLine;
        String output = "";
        while ((inputLine = in.readLine()) != null) {
        	output += inputLine + "\n";
        }
        in.close();
		return output;
	}
	
	
	
	}


