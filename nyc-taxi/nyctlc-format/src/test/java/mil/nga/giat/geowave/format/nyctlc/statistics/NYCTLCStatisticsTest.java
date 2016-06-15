package mil.nga.giat.geowave.format.nyctlc.statistics;

import java.io.File;

import org.junit.*;
import mil.nga.giat.geowave.format.nyctlc.NYCTLCIngestPlugin;
import mil.nga.giat.geowave.format.nyctlc.NYCTLCUtils;
import mil.nga.giat.geowave.format.nyctlc.avro.NYCTLCEntry;
import mil.nga.giat.geowave.format.nyctlc.statistics.NYCTLCStatistics.IntHistStat;

	//
	//
	//This test takes in a csv file, creates the Avro Objects, and uses the VendorID, RateCodeID, and PaymentType fields to create 3 IntHist Statistics
	//It tests to make sure that the Avro objects are created correctly and that the updateStat methods work correctly
	//If the number of reasonable values differs between the three statistics, it is because the update method threw out bad data
	//
	//

	public class NYCTLCStatisticsTest
	{

		public static File file = new File(
				"green_tripdata_2013-08.csv"); // enter the name of the csv file

		public static NYCTLCIngestPlugin nycplug = new NYCTLCIngestPlugin();
		public static NYCTLCEntry[] fields = nycplug.toAvroObjects(file); //creates the avro objects

		@Test
		public void testVendorIDAvro() {
			//Tests the Vendor Ids
			Boolean ans = true;
			IntHistStat vi = new IntHistStat(
					0,
					2);

			for (NYCTLCEntry entry : fields) {
				vi.updateStat(entry.getVendorId());

				for (long check : vi.counts) {
					if (check < 0 || check > 2) {
						ans = false;
					}
				}
			}
			int fb = vi.firstBin;
			System.out.println(""+'"'+(fb) +'"'+' '+ vi.counts[0]+"\n"+'"'+(fb+1) +'"'+' '+ vi.counts[1]+"\n"+'"'+(fb+2) +'"'+' '+ vi.counts[2]);
			System.out.println(vi.counts[0] + vi.counts[1] + vi.counts[2] + " reasonable Vendor IDs"+ "\n");

			assert (ans = true);
		}


		@Test
		public void testRateCodeIDAvro() {
			//Tests the Rate Code IDs
			Boolean ans = true;
			IntHistStat ri = new IntHistStat(
					1,
					6);

			for (NYCTLCEntry entry : fields) {
				ri.updateStat(entry.getRateCodeId());

				for (long check : ri.counts) {
					if (check < 1 || check > 6) {
						ans = false;
					}
				}
			}
			int fb = ri.firstBin;
		System.out.println(""+'"'+fb +'"'+' '+ ri.counts[0]+"\n"+'"'+(fb+1) +'"'+' '+ ri.counts[1]+"\n"+'"'+(fb+2) +'"'+' '+ ri.counts[2]+"\n"+'"'+(fb+3) +'"'+' '+ 
					ri.counts[3]+"\n"+'"'+(fb+4) +'"'+' '+ ri.counts[4]+"\n"+'"'+(fb+5) +'"'+' '+ ri.counts[5]);
			System.out.println(ri.counts[0] + ri.counts[1] + ri.counts[2] + ri.counts[3]
					+ ri.counts[4] + ri.counts[5] + " reasonable Rate Code IDs"+"\n");

			assert (ans = true);
		}

		@Test
		public void testPaymentTypeAvro() {
			//Tests the Payment Types
			Boolean ans = true;
			IntHistStat pt = new IntHistStat(
					0,
					6);

			for (NYCTLCEntry entry : fields) {
				pt.updateStat(entry.getPaymentType());

				for (long check : pt.counts) {
					if (check < 0 || check > 6) {
						ans = false;
					}
				}
			}
			int fb = pt.firstBin;
			System.out.println(""+'"'+fb +'"'+' '+ pt.counts[0]+"\n"+'"'+(fb+1) +'"'+' '+ pt.counts[1]+"\n"+'"'+(fb+2) +'"'+' '+ pt.counts[2]+"\n"+'"'+(fb+3) +'"'+' '+ 
					pt.counts[3]+"\n"+'"'+(fb+4) +'"'+' '+ pt.counts[4]+"\n"+'"'+(fb+5) +'"'+' '+ pt.counts[5]+"\n"+'"'+(fb+6) +'"'+' '+ pt.counts[6]);
			System.out.println(pt.counts[0] + pt.counts[1] + pt.counts[2] + pt.counts[3]
					+ pt.counts[4] + pt.counts[5] + pt.counts[6] + " reasonable Payment Types"+"\n");

			assert (ans = true);
		}

		@Test
		public void testCabTypeAvro() {
			//Tests the Cab Types
			Boolean ans = true;
			IntHistStat ct = new IntHistStat(
					0,
					2);

			for (NYCTLCEntry entry : fields) {
				ct.updateStat(entry.getCabType());

				for (long check : ct.counts) {
					if (check < 0 || check > 2) {
						ans = false;
					}
				}
			}
			int fb = ct.firstBin;
			System.out.println(""+'"'+fb +'"'+' '+ ct.counts[0]+"\n"+'"'+(fb+1) +'"'+' '+ ct.counts[1]+"\n"+'"'+(fb+2) +'"'+' '+ ct.counts[2]);
			System.out.println(ct.counts[0] + ct.counts[1] + ct.counts[2] + " reasonable Cab Types"+"\n");


			assert (ans = true);
		}

	
}
