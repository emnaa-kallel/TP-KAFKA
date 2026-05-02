package tn.utm.kafka;

public class EvenementPOS {
    private String type;
    private String idCaisse;
    private String ville;
    private String timestamp;
    private double montant;

    public EvenementPOS() {}

    public EvenementPOS(String type, String idCaisse, String ville, String timestamp, double montant) {
        this.type = type;
        this.idCaisse = idCaisse;
        this.ville = ville;
        this.timestamp = timestamp;
        this.montant = montant;
    }

    public String getType() { return type; }
    public String getIdCaisse() { return idCaisse; }
    public String getVille() { return ville; }
    public String getTimestamp() { return timestamp; }
    public double getMontant() { return montant; }

    public void setType(String type) { this.type = type; }
    public void setIdCaisse(String idCaisse) { this.idCaisse = idCaisse; }
    public void setVille(String ville) { this.ville = ville; }
    public void setTimestamp(String timestamp) { this.timestamp = timestamp; }
    public void setMontant(double montant) { this.montant = montant; }

    @Override
    public String toString() {
        return "EvenementPOS{type='" + type + "', idCaisse='" + idCaisse +
               "', ville='" + ville + "', montant=" + montant + "}";
    }
}