package org.apache.bookkeeper.bookie.storage.ldb;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.*;


@RunWith(Parameterized.class)
public class ReadCacheGetTest {

	/** Init ReadCache */
	private static ReadCache cache;
	private enum ConstantChecker {VALID_GET_CACHE, INVALID_GET_CACHE}
	private ConstantChecker testType;

	private static final ByteBufAllocator allocator = UnpooledByteBufAllocator.DEFAULT;


	/** Entry dimension: 1024 byte*/
	private static int ENTRY_SIZE = 1024;

	/** Max entries inside Cache*/
	private static int MAX_ENTRIES = 10;

	/** Cache total dimension */
	private static int CACHE_SIZE = ENTRY_SIZE*MAX_ENTRIES;

	/** Ledger Id */
	private long ledgerId;

	/** Entry Id */
	private long entryId;

	/** Entry to get from cache */
	private ByteBuf expectedResult;



	@Before
	public void configure() {
		this.expectedResult = allocator.buffer(ENTRY_SIZE);
		this.expectedResult.writeBytes("3ntry_t0_ch3ck".getBytes());

		cache = new ReadCache(allocator,CACHE_SIZE);
		cache.put(1, 1, this.expectedResult);
	}


	@AfterClass
	public static void closeCache() {
		cache.close();
	}


	@Parameters
	public static Collection<Object[]> getParameters()  {
		return Arrays.asList(new Object[][]{
				{-1, -1, ConstantChecker.INVALID_GET_CACHE},
				{-1, 0, ConstantChecker.INVALID_GET_CACHE},
				{-1, 1, ConstantChecker.INVALID_GET_CACHE},
				{0, -1, ConstantChecker.INVALID_GET_CACHE},
				{0, 0, ConstantChecker.INVALID_GET_CACHE},
				{0, 1, ConstantChecker.INVALID_GET_CACHE},
				{1, -1, ConstantChecker.INVALID_GET_CACHE},
				{1, 0, ConstantChecker.INVALID_GET_CACHE},
				{1, 1, ConstantChecker.VALID_GET_CACHE}
		});
	}


	public ReadCacheGetTest(long ledgerId, long entryId, ConstantChecker testType) {
		this.ledgerId = ledgerId;
		this.entryId = entryId;
		this.testType = testType;
	}
	

	@Test
	public void getTest() {

		try{
			ByteBuf actualValue = cache.get(ledgerId, entryId);

			/* Check if the returned entry is the correct entry to return, so the reading was successful */
			if(this.testType == ConstantChecker.VALID_GET_CACHE){assertEquals(this.expectedResult, actualValue);}
			if(this.testType == ConstantChecker.INVALID_GET_CACHE){
				assertNull(actualValue);
			}

		}catch(Exception e){
			if(this.testType == ConstantChecker.INVALID_GET_CACHE){
				System.out.println("\n\nECCEZZIONE --> " + e.getClass().getName() + "\n\n");
				assertTrue("Impossible to get te entry",true);
			}else{fail();}
		}
	}
}
