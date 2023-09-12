package org.apache.bookkeeper.client;

import org.apache.bookkeeper.client.conf.BookKeeperClusterTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import static org.junit.Assert.*;


@RunWith(Parameterized.class)
public class BookKeeperDeleteLedgerTest extends BookKeeperClusterTestCase {

    private enum ConstantChecker {VALID_DELETE, INVALID_DELETE}

    /** Ledger ID */
    private long ledgerIdToCheck;

    private ConstantChecker testType;

    /** BookKeeper Client */
    private BookKeeper bkKClient;


    @Before
    public void setUp() throws Exception{
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


    public BookKeeperDeleteLedgerTest(ConstantChecker testType, long ledgerId) {
        super(2, 120);

        this.testType = testType;
        this.ledgerIdToCheck = ledgerId;
    }


    @Parameterized.Parameters
    public static Collection<Object[]> getParameters()  {
        return Arrays.asList(new Object[][]{
                {ConstantChecker.INVALID_DELETE, -1},
                {ConstantChecker.VALID_DELETE, 0},
                {ConstantChecker.INVALID_DELETE, 1},
        });
    }



    @Test
    public void deleteLedgerTest() throws Exception {
        boolean testPassed = false;

        bkKClient.createLedger(1, 1, 1, BookKeeper.DigestType.CRC32, "p@SSw0rd".getBytes());

        try {
            bkKClient.deleteLedger(this.ledgerIdToCheck);

            /* Check if after deleting the ledger still exist */
            if(!checkExistLedgerId(bkKClient, this.ledgerIdToCheck)){ testPassed=true; }

        } catch (Exception e) {
            if(testType == ConstantChecker.INVALID_DELETE){
                testPassed=true;
            }else{fail();}
        }

        Assert.assertTrue("The ledger was not successfully deleted", testPassed);
    }




    /** Prende la lista dei ledger correnti riferiti al client passato, e controlla se tra questi
     * ne è presente uno con l'id passato come parametro. */
    public boolean checkExistLedgerId(BookKeeper bkClient, long ledgerId) throws Exception {

        BookKeeperAdmin bkAdmin = new BookKeeperAdmin(bkClient);

        /* List of ledger in the cluster  */
        Iterable<Long> ledgerIds =  bkAdmin.listLedgers();

        /* Check the ledger ID in the Iterable ledgers list  */
        for (long id : ledgerIds) {
            if(ledgerId == id) return true;
        }

        bkAdmin.close();
        return false;
    }



}