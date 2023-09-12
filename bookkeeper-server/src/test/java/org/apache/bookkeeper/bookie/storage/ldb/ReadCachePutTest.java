package org.apache.bookkeeper.bookie.storage.ldb;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;


@RunWith(Parameterized.class)
public class ReadCachePutTest {

	/** Init ReadCache */
	private static ReadCache cache;
	private enum ConstantChecker {VALID_PUT_ENTRY, INVALID_PUT_ENTRY, EXCEED_DIM_ENTRY}
	private ConstantChecker testType;

	/** Entry dimension: 1024 byte*/
	private static int ENTRY_SIZE = 1024;

	/** Max entries inside Cache*/
	private static int MAX_ENTRIES = 50;

	/** Cache total dimension */
	private static int CACHE_SIZE = ENTRY_SIZE*MAX_ENTRIES;

	/** Ledger Id */
	private long ledgerId;

	/** Entry Id */
	private long entryId;

	/** Entry to put */
	private ByteBuf entry;


	private static int NUM_PUT = MAX_ENTRIES;
	private boolean stressPut;



	public ReadCachePutTest(long ledgerId, long entryId, ByteBuf entryToPut, ConstantChecker testType, boolean stressPut) {
		this.ledgerId = ledgerId;
		this.entryId = entryId;
		this.entry = entryToPut;
		this.testType = testType;
		this.stressPut = stressPut;
	}



	@Before
	public void configure() {
		UnpooledByteBufAllocator allocator = UnpooledByteBufAllocator.DEFAULT;
		cache = new ReadCache(allocator,CACHE_SIZE);
	}


	@AfterClass
	public static void closeCache() {cache.close();}


	@Parameters
	public static Collection<Object[]> getParameters()  {
		ByteBuf valid_entry = Unpooled.wrappedBuffer(new byte[ENTRY_SIZE]);
		ByteBuf exceedDim_entry = Unpooled.wrappedBuffer(new byte[CACHE_SIZE+1]);

		valid_entry.writerIndex(valid_entry.capacity());
		exceedDim_entry.writerIndex(exceedDim_entry.capacity());


		return Arrays.asList(new Object[][]{
				{-1, -1, valid_entry, ConstantChecker.INVALID_PUT_ENTRY, false},
				{-1, 0, valid_entry, ConstantChecker.INVALID_PUT_ENTRY, false},
				{-1, 1, valid_entry, ConstantChecker.INVALID_PUT_ENTRY, false},
				{0, -1, valid_entry, ConstantChecker.VALID_PUT_ENTRY, false},
				{0, 0, null, ConstantChecker.INVALID_PUT_ENTRY, false},
				{0, 1, exceedDim_entry, ConstantChecker.EXCEED_DIM_ENTRY, false},
				{1, -1, valid_entry, ConstantChecker.VALID_PUT_ENTRY, true},
				{1, 0, null, ConstantChecker.INVALID_PUT_ENTRY, false},
				{1, 1, valid_entry, ConstantChecker.VALID_PUT_ENTRY, false}
		});
	}



	@Test
	public void putTest() {
		int n=1;

		try{
			if (this.stressPut) {
				for (n=0;n<NUM_PUT;n++) {
					cache.put(ledgerId, n, entry);
				}
			}
			else {
				cache.put(ledgerId, entryId, entry);
			}

			System.out.println(cache.count());

			/* If the entry exceed te dimension of cache,skip the put */
			if((cache.count() == (long) 0) && (this.testType == ConstantChecker.EXCEED_DIM_ENTRY)) n=0;

			/* Check if cache has n entry */
			assertEquals(n, cache.count());
			/* Check if the dimension of cache is update with new putted entries */
			assertEquals(n*ENTRY_SIZE, cache.size());

		}catch (Exception e){
			if(this.testType == ConstantChecker.INVALID_PUT_ENTRY){
				Assert.assertTrue("Impossible to put the entry", true);
			}else{fail();}
		}


	}
}
