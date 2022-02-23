/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Main.java to edit this template
 */
package Database;

import java.sql.SQLException;

/**
 *
 * @author march
 */
public class TestConnessione {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws SQLException {
        // TODO code application logic here

        System.out.println("Sto provando ad effettuare la connessione al DB siap");
        DBconnection.getInstance().connect();
        System.out.println("Mi sono riuscito a connettere");

        //        DBconnection.getInstance().elencoGare();
        //    int valore = DBconnection.getInstance().num_gare_vinte("01998810244");
        //    System.out.println("Il numero delle gare vinte è " + valore);
        //    DBconnection.getInstance().valore_relativo_bando_over40k_perAppaltatore("System.out.println(sql);
        //    DBconnection.getInstance().valore_relativo_bando_under40k_perAppaltatore("01998810244");
        //DBconnection.getInstance().valore_relativo_bando_over40k_perCig("0084569C88");
        // DBconnection.getInstance().valore_relativo_bando_over40k_perAppaltatore_perIntervalloAnno("01998810244", "2009" , "2010");
        //DBconnection.getInstance().intervallo_delle_offerte_over40k_perAppaltatore("01998810244");
        //DBconnection.getInstance().intervallo_delle_offerte_over40k_perCig("0141055244");
        // DBconnection.getInstance().intervallo_delle_offerte_over40k_perAppaltatore_perIntervalloAnno("01998810244", "2009", "2010");
        DBconnection.getInstance().mercato_concentrato("01998810244", "55524000-9");
        DBconnection.getInstance().disconnect();
        System.out.println("Ho effettualo la DISCONNESSIONE");
    }

}
