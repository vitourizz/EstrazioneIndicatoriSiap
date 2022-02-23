/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package Database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;

/**
 *
 * @author march
 */
public class DBconnection {

    private static DBconnection instance;

    private Connection connection;

    public DBconnection() {
    }

    public static DBconnection getInstance() {
        if (instance == null) {
            instance = new DBconnection();
        }
        return instance;
    }

// Metodo che si occupa di effettuare la connessione al DB
    public void connect() throws SQLException {
        Properties dbprops = new Properties();
        dbprops.setProperty("user", "siap_user");
        dbprops.setProperty("password", "U@&GZ8UJMz");
        connection = DriverManager.getConnection("jdbc:h2:./resources/db/siap", dbprops);
    }

// Metodo che si occupa di effettuare la disconnessione dal DB
    public void disconnect() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    // Metodo che restituisce il numero di gare vinte da un determinato appaltatore dato in inout tramite il suo codice fiscale (String).
    public int num_gare_vinte(String codiceFiscale) throws SQLException {
        String sql;
        String codice_fiscale = codiceFiscale;
        sql = "SELECT COUNT (cig) FROM SIAP.AGGIUDICATARI WHERE SIAP.AGGIUDICATARI.CODICE_FISCALE = '" + codice_fiscale + "'";
        Statement gare_vinte = connection.createStatement();
        ResultSet rs = gare_vinte.executeQuery(sql);
        int numero_gare = 0;
        while (rs.next()) {
            numero_gare = rs.getInt(1);
        }
        return numero_gare;
    }

    /*  public void prima_parte() throws SQLException {
        String sql;
        sql = "SELECT SIAP.AGGIUDICAZIONI.CIG ,SIAP.AGGIUDICAZIONI.IMPORTO_AGGIUDICAZIONE FROM SIAP.AGGIUDICAZIONI  JOIN SIAP.AGGIUDICATARI  ON SIAP.AGGIUDICAZIONI.CIG = SIAP.AGGIUDICATARI.CIG WHERE SIAP.AGGIUDICATARI.CODICE_FISCALE='01998810244'";
        System.out.println(sql);
        Statement gare_vinte = connection.createStatement();
        ResultSet rs = gare_vinte.executeQuery(sql);

        while (rs.next()) {
            System.out.println(rs.getString(1) + "-----------" + rs.getString(2));
        }
    }

     */
// Metodo che calola l'inficatore "valore relativo bando" facendone la media, di ogni appaltatore , tramite il suo codice fiscale dato in unput (String).
// In riferimento alla gare con valore totale SOPRA i 40 mila euro.
    public void valore_relativo_bando_over40k_perAppaltatore(String codiceFiscale) throws SQLException {
        String codice_fiscale = codiceFiscale;
        String sql;
        sql = "SELECT SIAP.AGGIUDICAZIONI.IMPORTO_AGGIUDICAZIONE, SIAP.GARE.IMPORTO_COMPLESSIVO_GARA FROM (SIAP.AGGIUDICAZIONI JOIN SIAP.AGGIUDICATARI ON SIAP.AGGIUDICAZIONI.CIG = SIAP.AGGIUDICATARI.CIG) JOIN SIAP.GARE ON SIAP.AGGIUDICAZIONI.CIG = SIAP.GARE.CIG WHERE SIAP.AGGIUDICATARI.CODICE_FISCALE ='" + codice_fiscale + "'";
        System.out.println(sql);
        Statement gare_vinte = connection.createStatement();
        ResultSet rs = gare_vinte.executeQuery(sql);

        List<Double> importo_aggiudicazione = new ArrayList<>();
        List<Double> importo_complessivo = new ArrayList<>();

        while (rs.next()) {
            double a = rs.getDouble(1);
            double b = rs.getDouble(2);
            importo_aggiudicazione.add(a);
            importo_complessivo.add(b);

        }

        double rapporto = 0;
        for (int i = 0; i < importo_aggiudicazione.size(); i++) {
            rapporto = rapporto + (importo_aggiudicazione.get(i) / importo_complessivo.get(i));
        }
        rapporto = rapporto / importo_complessivo.size();
        System.out.println("Il rapporto per questo appaltatore, per le gare superiori a 40K, è : " + rapporto);
    }

// Metodo che calola l'inficatore "valore relativo bando" facendone la media, di ogni appaltatore , tramite il suo codice fiscale dato in unput (String).
// In riferimento alla gare con valore totale SOTTO i 40 mila euro.
    public void valore_relativo_bando_under40k_perAppaltatore(String codiceFiscale) throws SQLException {
        String codice_fiscale = codiceFiscale;
        String sql;
        sql = "SELECT SIAP.AGGIUDICAZIONI.IMPORTO_AGGIUDICAZIONE, SIAP.GARE_SMARTIG.IMPORTO_COMPLESSIVO_GARA FROM (SIAP.AGGIUDICAZIONI JOIN SIAP.AGGIUDICATARI ON SIAP.AGGIUDICAZIONI.CIG = SIAP.AGGIUDICATARI.CIG) JOIN SIAP.GARE_SMARTIG ON SIAP.AGGIUDICAZIONI.CIG = SIAP.GARE_SMARTIG.CIG WHERE SIAP.AGGIUDICATARI.CODICE_FISCALE ='" + codice_fiscale + "'";
        System.out.println(sql);
        Statement gare_vinte = connection.createStatement();
        ResultSet rs = gare_vinte.executeQuery(sql);

        List<Double> importo_aggiudicazione = new ArrayList<>();
        List<Double> importo_complessivo = new ArrayList<>();

        while (rs.next()) {
            double a = rs.getDouble(1);
            double b = rs.getDouble(2);
            importo_aggiudicazione.add(a);
            importo_complessivo.add(b);

        }

        double rapporto = 0;
        for (int i = 0; i < importo_aggiudicazione.size(); i++) {
            rapporto = rapporto + (importo_aggiudicazione.get(i) / importo_complessivo.get(i));
        }
        rapporto = rapporto / importo_complessivo.size();
        System.out.println("Il rapporto per questo appaltatore riguardo alle gare sotto i 40 K, è : " + rapporto);
    }

// Metodo che calcola il valore relativo del bando, dato un CIG in input. Tenendo traccia anche del codice fiscale dell'aggiudicatario di tale gara.
    public void valore_relativo_bando_over40k_perCig(String Cig) throws SQLException {
        String codice_Cig = Cig;
        String sql;
        sql = "SELECT SIAP.AGGIUDICAZIONI.IMPORTO_AGGIUDICAZIONE, SIAP.GARE.IMPORTO_COMPLESSIVO_GARA, SIAP.AGGIUDICATARI.CODICE_FISCALE FROM (SIAP.AGGIUDICAZIONI JOIN SIAP.GARE ON SIAP.AGGIUDICAZIONI.CIG = SIAP.GARE.CIG) JOIN SIAP.AGGIUDICATARI ON SIAP.AGGIUDICAZIONI.CIG = SIAP.AGGIUDICATARI.CIG WHERE SIAP.AGGIUDICAZIONI.CIG ='" + codice_Cig + "'";
        System.out.println(sql);
        Statement valore_per_cig = connection.createStatement();
        ResultSet rs = valore_per_cig.executeQuery(sql);
        double rapporto_singolo = 0;
        double importo_aggiudicazione = 0;
        double importo_gara = 0;
        String codice_fiscale_appaltatore = null;
        while (rs.next()) {
            System.out.println(rs.getInt(1) + "-----" + rs.getInt(2));
            importo_aggiudicazione = rs.getDouble(1);
            importo_gara = rs.getDouble(2);
            codice_fiscale_appaltatore = rs.getString(3);

        }
        rapporto_singolo = (importo_aggiudicazione / importo_gara);
        System.out.println("La gara con CIG: " + codice_Cig + " vinta dall'appaltatore con codice fiscale: " + codice_fiscale_appaltatore + " ha l'indicatore del VRB pari a: " + rapporto_singolo);
    }

// Metodo che calcola il VRB medio di un appaltatore relativo a un intervallo di tempo
    public void valore_relativo_bando_over40k_perAppaltatore_perIntervalloAnno(String codiceFiscale, String annoInizio, String annoFine) throws SQLException {
        String codice_fiscale = codiceFiscale;
        String anno_inizio = annoInizio;
        String anno_fine = annoFine;

        String sql;
        sql = "SELECT SIAP.AGGIUDICAZIONI.IMPORTO_AGGIUDICAZIONE, SIAP.GARE.IMPORTO_COMPLESSIVO_GARA FROM (SIAP.AGGIUDICAZIONI JOIN SIAP.AGGIUDICATARI ON SIAP.AGGIUDICAZIONI.CIG = SIAP.AGGIUDICATARI.CIG) JOIN SIAP.GARE ON SIAP.AGGIUDICAZIONI.CIG = SIAP.GARE.CIG WHERE SIAP.AGGIUDICATARI.CODICE_FISCALE ='" + codice_fiscale + "' and YEAR(SIAP.GARE.DATA_PUBBLICAZIONE) >='" + anno_inizio + "' and YEAR(SIAP.GARE.DATA_PUBBLICAZIONE) <='" + anno_fine + "'";
        System.out.println(sql);
        Statement gare_vinte = connection.createStatement();
        ResultSet rs = gare_vinte.executeQuery(sql);

        List<Double> importo_aggiudicazione = new ArrayList<>();
        List<Double> importo_complessivo = new ArrayList<>();

        while (rs.next()) {
            double a = rs.getInt(1);
            double b = rs.getInt(2);
            importo_aggiudicazione.add(a);
            importo_complessivo.add(b);

        }

        double rapporto = 0;
        for (int i = 0; i < importo_aggiudicazione.size(); i++) {
            rapporto = rapporto + (importo_aggiudicazione.get(i) / importo_complessivo.get(i));
        }
        rapporto = rapporto / importo_complessivo.size();
        System.out.println("Il VRB per l'appaltatore con codice fiscale: " + codice_fiscale + " , per le gare superiori a 40K, relativo all'intervallo di tempo che va dall'anno: " + anno_inizio + " all'anno: " + anno_fine + " è pari a: " + rapporto);
    }

// Metodo che calola l'indicatore "intervallo delle offerte" facendone la media, di ogni appaltatore , tramite il suo codice fiscale dato in unput (String).
// In riferimento alla gare con valore totale SOPRA i 40 mila euro.
    public void intervallo_delle_offerte_over40k_perAppaltatore(String codiceFiscale) throws SQLException {
        String codice_fiscale = codiceFiscale;
        String sql;
        sql = "SELECT SIAP.AGGIUDICAZIONI.MASSIMO_RIBASSO, SIAP.AGGIUDICAZIONI.MINIMO_RIBASSO FROM (SIAP.AGGIUDICAZIONI JOIN SIAP.AGGIUDICATARI ON SIAP.AGGIUDICAZIONI.CIG = SIAP.AGGIUDICATARI.CIG) JOIN SIAP.GARE ON SIAP.AGGIUDICAZIONI.CIG = SIAP.GARE.CIG WHERE SIAP.AGGIUDICATARI.CODICE_FISCALE ='" + codice_fiscale + "'";
        System.out.println(sql);
        Statement gare_vinte = connection.createStatement();
        ResultSet rs = gare_vinte.executeQuery(sql);

        List<Double> massimo_ribasso = new ArrayList<>();
        List<Double> minimo_ribasso = new ArrayList<>();

        while (rs.next()) {
            double a = rs.getDouble(1);
            double b = rs.getDouble(2);
            massimo_ribasso.add(a);
            minimo_ribasso.add(b);

        }
        System.out.println(massimo_ribasso);
        System.out.println(minimo_ribasso);

        double varianzaOfferte = 0;
        for (int i = 0; i < massimo_ribasso.size(); i++) {
            varianzaOfferte = varianzaOfferte + (massimo_ribasso.get(i) - minimo_ribasso.get(i));
        }
        varianzaOfferte = varianzaOfferte / massimo_ribasso.size();
        System.out.println("La varianza media delle offerte per questo appaltatore, per le gare superiori a 40K, è : " + varianzaOfferte);
    }

// Metodo che calcola l'indicatore "intervallo delle offerte" dato un CIG in input. Tenendo traccia anche del codice fiscale dell'aggiudicatario di tale gara.
    public void intervallo_delle_offerte_over40k_perCig(String Cig) throws SQLException {
        String codice_Cig = Cig;
        String sql;
        sql = "SELECT SIAP.AGGIUDICAZIONI.MASSIMO_RIBASSO, SIAP.AGGIUDICAZIONI.MINIMO_RIBASSO, SIAP.AGGIUDICATARI.CODICE_FISCALE FROM (SIAP.AGGIUDICAZIONI JOIN SIAP.GARE ON SIAP.AGGIUDICAZIONI.CIG = SIAP.GARE.CIG) JOIN SIAP.AGGIUDICATARI ON SIAP.AGGIUDICAZIONI.CIG = SIAP.AGGIUDICATARI.CIG WHERE SIAP.AGGIUDICAZIONI.CIG ='" + codice_Cig + "'";
        System.out.println(sql);
        Statement valore_per_cig = connection.createStatement();
        ResultSet rs = valore_per_cig.executeQuery(sql);
        double varianzaOfferta_singola = 0;
        double massimo_ribasso = 0;
        double minimo_ribasso = 0;
        String codice_fiscale_appaltatore = null;
        while (rs.next()) {
            System.out.println(rs.getDouble(1) + "-----" + rs.getDouble(2));
            massimo_ribasso = rs.getDouble(1);
            minimo_ribasso = rs.getDouble(2);
            codice_fiscale_appaltatore = rs.getString(3);

        }
        varianzaOfferta_singola = (massimo_ribasso - minimo_ribasso);
        System.out.println("La gara con CIG: " + codice_Cig + " vinta dall'appaltatore con codice fiscale: " + codice_fiscale_appaltatore + " ha l'indicatore del IDO pari a: " + varianzaOfferta_singola);
    }

// Metodo che calcola l'indicatore "intervallo delle offerte" medio di un appaltatore relativo a un intervallo di tempo 
    public void intervallo_delle_offerte_over40k_perAppaltatore_perIntervalloAnno(String codiceFiscale, String annoInizio, String annoFine) throws SQLException {
        String codice_fiscale = codiceFiscale;
        String anno_inizio = annoInizio;
        String anno_fine = annoFine;

        String sql;
        sql = "SELECT SIAP.AGGIUDICAZIONI.MASSIMO_RIBASSO, SIAP.AGGIUDICAZIONI.MINIMO_RIBASSO FROM (SIAP.AGGIUDICAZIONI JOIN SIAP.AGGIUDICATARI ON SIAP.AGGIUDICAZIONI.CIG = SIAP.AGGIUDICATARI.CIG) JOIN SIAP.GARE ON SIAP.AGGIUDICAZIONI.CIG = SIAP.GARE.CIG WHERE SIAP.AGGIUDICATARI.CODICE_FISCALE ='" + codice_fiscale + "' and YEAR(SIAP.GARE.DATA_PUBBLICAZIONE) >='" + anno_inizio + "' and YEAR(SIAP.GARE.DATA_PUBBLICAZIONE) <='" + anno_fine + "'";
        System.out.println(sql);
        Statement gare_vinte = connection.createStatement();
        ResultSet rs = gare_vinte.executeQuery(sql);

        List<Double> massimo_ribasso = new ArrayList<>();
        List<Double> minimo_ribasso = new ArrayList<>();

        while (rs.next()) {
            double a = rs.getDouble(1);
            double b = rs.getDouble(2);
            massimo_ribasso.add(a);
            minimo_ribasso.add(b);
        }

        double varianzaOfferte = 0;
        for (int i = 0; i < massimo_ribasso.size(); i++) {
            varianzaOfferte = varianzaOfferte + (massimo_ribasso.get(i) - minimo_ribasso.get(i));
        }
        varianzaOfferte = varianzaOfferte / massimo_ribasso.size();
        System.out.println("Il IDO per l'appaltatore con codice fiscale: " + codice_fiscale + " , per le gare superiori a 40K, relativo all'intervallo di tempo che va dall'anno: " + anno_inizio + " all'anno: " + anno_fine + " è pari a: " + varianzaOfferte);
    }

// metodo che clacola l'indicatore del mercato concentrato, andando a fare il rapporto tra la somma degli importi di aggiudicazione di un determinato
// appaltatore (con codice fiscale in input) rigurdanti un determinato cpv (dato iu input) e la somma degli importi di aggiudicazione
// di tutte le gare riguardanti il cpv dato in input.(Livello massimo di dettaglio del CPV)
    public void mercato_concentrato(String codiceFiscale, String cpvMercatoRiferimento) throws SQLException {

        String codice_fiscale = codiceFiscale;
        String CPV = cpvMercatoRiferimento;

        String sql = "SELECT SIAP.AGGIUDICAZIONI.IMPORTO_AGGIUDICAZIONE FROM (SIAP.AGGIUDICAZIONI JOIN SIAP.AGGIUDICATARI ON SIAP.AGGIUDICAZIONI.CIG = SIAP.AGGIUDICATARI.CIG) JOIN SIAP.GARE ON SIAP.AGGIUDICAZIONI.CIG = SIAP.GARE.CIG WHERE SIAP.AGGIUDICATARI.CODICE_FISCALE ='" + codice_fiscale + "' and SIAP.GARE.COD_CPV='" + CPV + "'";
        System.out.println(sql);
        Statement importi_gare_vinte = connection.createStatement();
        ResultSet rs = importi_gare_vinte.executeQuery(sql);

        double totale = 0;
        while (rs.next()) {

            totale = totale + (rs.getDouble(1));
        }
        System.out.println(totale);

        String sql2 = "SELECT SIAP.AGGIUDICAZIONI.IMPORTO_AGGIUDICAZIONE FROM SIAP.AGGIUDICAZIONI JOIN SIAP.GARE ON SIAP.AGGIUDICAZIONI.CIG = SIAP.GARE.CIG WHERE SIAP.GARE.COD_CPV='" + CPV + "'";

        Statement importi_gare_cpv = connection.createStatement();
        ResultSet rs2 = importi_gare_cpv.executeQuery(sql2);
        double totalePerCpv = 0;
        while (rs2.next()) {
            totalePerCpv = totalePerCpv + rs2.getDouble(1);
        }

        System.out.println(totalePerCpv);

        double mercatoConcentrato = totale / totalePerCpv;
        System.out.println(mercatoConcentrato);
    }

}

/*


public void elencoGare() throws SQLException {
        /* ArrayList<Cliente> elenco = new ArrayList<Cliente>();
    String sql;
    sql = "SELECT ANAG_ID, nome, CLI_EMAIL FROM anagraficacliente";

    
    ResultSet rs;
    Statement stm = connection.createStatement();
    //st = cn.createStatement();
    rs = stm.executeQuery(sql);
        while (rs.next() == true) {
        Cliente a = new Cliente (rs.getInt("ANAG_ID"), rs.getString("nome"), rs.getString("CLI_EMAIL"));
        elenco.add(a);
        } return elenco;
}   

    Scanner input = new Scanner(System.in);
    System.out.println("Inserisci il codice fiscale dell'appaltatore da cercare");
    String codice = input.nextLine();

        String sql, sql2, sql3;
        sql = "SELECT cig , codice_fiscale FROM SIAP.AGGIUDICATARI WHERE SIAP.AGGIUDICATARI.CODICE_FISCALE ='" + codice +"'";
        System.out.println(sql);
       // sql2 = "SELECT SIAP.AGGIUDICATARI.cig, codice_fiscale FROM SIAP.AGGIUDICATARI, SIAP.AGGIUDICAZIONI WHERE SIAP.AGGIUDICATARI.ID_AGGIUDICAZIONE=SIAP.AGGIUDICAZIONI.ID_AGGIUDICAZIONE";
        Statement stm = connection.createStatement();
        ResultSet rs = stm.executeQuery(sql);


    System.out.println("provo a contare il numero di occorrenze");

        sql3 = "select count (cig) FROM SIAP.AGGIUDICATARI WHERE SIAP.AGGIUDICATARI.CODICE_FISCALE = '01998810244'";
        Statement stm3 = connection.createStatement();
        ResultSet rs3 = stm3.executeQuery(sql3);
        while (rs3.next()) {
             System.out.println(rs3.getInt(1));
          
            }

            int numero_gare_vinte = rs3.getInt(1);
            System.out.println(numero_gare_vinte);
  
        while (rs.next()) {
            System.out.println(rs.getString(1) + ": " + rs.getString(2));
     
           
        }

//sql3 = "SELECT SUM importo_aggiudicazione FROM SIAP.AGGIUDICAZIONI WHERE SIAP.AGGIUDICAZIONI.CIG =";

    System.out.println("provo a stampare la stringa dei CIG");
//String stringa_cig = rs.getString(1);

  //  System.out.println(stringa_cig);
  
        rs.close();
        stm.close();
    } 
}

 */
