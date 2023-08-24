package org.apache.bookkeeper.client;

import org.apache.bookkeeper.client.conf.BookKeeperClusterTestCase;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import static org.junit.Assert.*;


@RunWith(Parameterized.class)
public class BookKeeperOpenLedgerTest extends BookKeeperClusterTestCase {

    private enum ConstantChecker {INVALID_OPEN, VALID_OPEN}


    /** The default digest type used for opening ledgers. (CRC32) */
    private BookKeeper.DigestType digestType;

    /** Password used to open ledger */
    private byte[] passwordToUse;

    /** Id of ledger to open */
    private long idToUse;


    private ConstantChecker testType;

    /** BookKeeper Client */
    private BookKeeper bkKClient;



    @Before
    public void setUp() throws Exception {
        baseConf.setJournalWriteData(true);
        baseClientConf.setUseV2WireProtocol(true);
        super.setUp();
        bkKClient = new BookKeeper(baseClientConf);
    }

    @Override @After
    public void tearDown() throws Exception {
        super.tearDown();

        bkKClient.close();
        zkc.close();
    }

    @Parameterized.Parameters
    public static Collection<Object[]> getParameters()  {
        return Arrays.asList(new Object[][]{
                {ConstantChecker.VALID_OPEN, 0, BookKeeper.DigestType.CRC32, "p@SSw0rd".getBytes()},
                {ConstantChecker.INVALID_OPEN, -1, BookKeeper.DigestType.DUMMY, null},
                {ConstantChecker.INVALID_OPEN, 0, BookKeeper.DigestType.CRC32, "n0t-p@SSw0rd".getBytes()},
                {ConstantChecker.INVALID_OPEN, 1, BookKeeper.DigestType.CRC32C, new byte[]{}},
                // {ConstantChecker.INVALID_OPEN, 0, BookKeeper.DigestType.MAC, new byte[Integer.MAX_VALUE]}
        });
    }


    public BookKeeperOpenLedgerTest(ConstantChecker testType, long idToUse, BookKeeper.DigestType digestType, byte[] passwdToUse)  {
        super(2, 70);

        this.testType = testType;
        this.idToUse = idToUse;
        this.digestType = digestType;
        this.passwordToUse = passwdToUse;
    }



    @Test
    public void openLedgerTest() throws BKException, InterruptedException {

        LedgerHandle handle = bkKClient.createLedger(1,1,1, BookKeeper.DigestType.CRC32, "p@SSw0rd".getBytes(), null);

        try {
            String entryToAdd = "R@nd0m 1nf0rm@t10n";

            handle.addEntry(entryToAdd.getBytes());
            handle.close();

            LedgerHandle newHandle = bkKClient.openLedger(this.idToUse, this.digestType, this.passwordToUse);
            LedgerEntry entry = newHandle.readLastEntry();
            byte[] entryContent = entry.getEntry();

            boolean entryCorrect = Arrays.equals(entryToAdd.getBytes(), entryContent);

            Assert.assertTrue("The ledger was successfully opened", entryCorrect);
            newHandle.close();

        }catch (Exception e){
            if(testType == ConstantChecker.INVALID_OPEN){
                System.out.println("\n\nECCEZZIONE --> " + e.getClass().getName() + "\n\n");
                Assert.assertTrue("Impossible to open ledger", true);
            }else{fail();}
        }
    }



}