package org.apache.bookkeeper.client;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.client.conf.BookKeeperClusterTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.jupiter.api.BeforeAll;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.fail;


@RunWith(Parameterized.class)
public class BookKeeperCreateLedgerIncrementTest extends BookKeeperClusterTestCase {

    private enum ConstantChecker {VALID_LEDGER, INVALID_LEDGER, INVALID_LEDGER_BUT_VALID_FORONLYPASSWORD, NO_META, NO_AQS, ONLY_PASSWORD}


    /** The number of nodes the ledger is stored on */
    private int ensSize;

    /** The number of nodes each entry is written to. In effect, the max replication for the entry. */
    private int wQS;

    /** The number of nodes an entry must be acknowledged on. In effect, the minimum replication for the entry. */
    private int aQS;

    /** The default digest type used for opening ledgers. (CRC32) */
    private BookKeeper.DigestType digestType;

    /** The default password used for opening ledgers. Default value is empty string. */
    private byte[] password;


    private ConstantChecker testType;

    /** BookKeeper Client */
    private BookKeeper bkKClient;


    @BeforeAll
    public void setUp() throws Exception {
        baseConf.setJournalWriteData(true);
        baseClientConf.setUseV2WireProtocol(true);

        baseClientConf.enableBookieHealthCheck();

        super.setUp();
        bkKClient = new BookKeeper(baseClientConf);
    }

    @Override @After
    public void tearDown() throws Exception {
        super.tearDown();

    }


    public BookKeeperCreateLedgerIncrementTest(ConstantChecker testType, int ensSize, int writeQuorumSize, int ackQuorumSize, BookKeeper.DigestType digestType, byte[] passwd){
        super(3, 200);

        this.testType = testType;
        this.ensSize = ensSize;
        this.wQS = writeQuorumSize;
        this.aQS = ackQuorumSize;
        this.digestType = digestType;
        this.password = passwd;
    }


    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() {
        /*
        * INVALID_LEDGER_BUT_VALID_FORONLYPASSWORD: un input invalido a causa dei valori di ensSize, wQS e aQS non è invalido per il metodo
        * public LedgerHandle createLedger(DigestType digestType, byte[] passwd) dato che non gli vengono passati; per indicare questo scenario
        * si è introdotto la seguente costante enum.
        * */
        return Arrays.asList(new Object[][]{
                           {ConstantChecker.VALID_LEDGER,0, -1, -2, BookKeeper.DigestType.CRC32C, "p@SSw0rd".getBytes()},
                      //   {ConstantChecker.VALID_LEDGER,-1, 0, 0, BookKeeper.DigestType.CRC32, new byte[Integer.MAX_VALUE]},
                           {ConstantChecker.INVALID_LEDGER_BUT_VALID_FORONLYPASSWORD,0, 0, 1, BookKeeper.DigestType.MAC, new byte[]{}},
                           {ConstantChecker.INVALID_LEDGER,1, 2, 3, BookKeeper.DigestType.DUMMY, null}
        });
    }





    /**
     * TESTS FOR METHOD WITHOUT CUSTOM METADATA PARAMETER
     * ------------------------------------------------
     * public LedgerHandle createLedger(int ensSize, int qSize, DigestType digestType, byte[] passwd) */

    @Test
    public void createLedgerNoMetaTest(){
        try {
            LedgerHandle handle = bkKClient.createLedger(ensSize, wQS, aQS, digestType, password);
            LedgerMetadata ledgerMetadata = handle.getLedgerMetadata();

            boolean correctlyConfigured = checkLedgerMetadata(ledgerMetadata, ConstantChecker.NO_META);

            Assert.assertTrue("The ledger was created successfully", correctlyConfigured);
        } catch (Exception e) {
            if(testType != ConstantChecker.VALID_LEDGER){
                System.out.println("\n\nIVALID_SENZAMETADATA_PRESO_TEST ---> EXCEPTION: " + e.getClass().getName() + "\n\n");
                Assert.assertTrue("The ledger could not be created",true);
            }
            else{fail();}
        }
    }

    /**
     * TESTS FOR METHOD WITHOUT AQS AND META
     * ------------------------------------------------
     * public LedgerHandle createLedger(int ensSize, int qSize, DigestType digestType, byte[] passwd) */
    @Test
    public void createLedgerNoAQSTest(){

        try {
            LedgerHandle handle = bkKClient.createLedger(ensSize, wQS, digestType, password);
            LedgerMetadata ledgerMetadata = handle.getLedgerMetadata();
            boolean correctlyConfigured = checkLedgerMetadata(ledgerMetadata, ConstantChecker.NO_AQS);

            Assert.assertTrue("The ledger was created successfully", correctlyConfigured);

        } catch (Exception e) {
            System.out.println("ENTRATO");

            if(testType != ConstantChecker.VALID_LEDGER){
                System.out.println("\n\nIVALID_CONMETADATA_PRESO_TEST ---> EXCEPTION: " + e.getClass().getName() + "\n\n");

                Assert.assertTrue("The ledger could not be created",true);
            }else{fail();}
        }

    }


    /**
     * TESTS FOR METHOD WITH ONLY PASSWORD
     * ------------------------------------------------
     * public LedgerHandle createLedger(DigestType digestType, byte[] passwd) */
    @Test
    public void createLedgerOnlyPasswordTest(){

        try {
            LedgerHandle handle = bkKClient.createLedger(digestType, password);
            LedgerMetadata ledgerMetadata = handle.getLedgerMetadata();
            boolean correctlyConfigured = checkLedgerMetadata(ledgerMetadata, ConstantChecker.ONLY_PASSWORD);

            Assert.assertTrue("The ledger was created successfully", correctlyConfigured);

        } catch (Exception e) {
            System.out.println("ENTRATO");

            if(testType == ConstantChecker.INVALID_LEDGER){
                System.out.println("\n\nIVALID_CONMETADATA_PRESO_TEST ---> EXCEPTION: " + e.getClass().getName() + "\n\n");

                Assert.assertTrue("The ledger could not be created",true);
            }else{fail();}
        }

    }



    /**
     * TESTING UTIL */

    private boolean checkLedgerMetadata(LedgerMetadata metadata, ConstantChecker methodType) {
        if (methodType != ConstantChecker.ONLY_PASSWORD && metadata.getEnsembleSize() != ensSize) return false;
        if (methodType != ConstantChecker.ONLY_PASSWORD && metadata.getWriteQuorumSize() != wQS) return false;
        if (methodType == ConstantChecker.NO_META && metadata.getAckQuorumSize() != aQS) return false;
        if (!Arrays.equals(metadata.getPassword(), password)) return false;

        return true;
    }

}