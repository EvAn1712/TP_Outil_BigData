package org.epf.hadoop.colfil3;

import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class UserRecommendation implements WritableComparable<UserRecommendation> {
    private String userID;         // Utilisateur
    private String recommendationID;  // Utilisateur recommandé
    private int amisCommun;    // Nombre d'amis en commun

    public UserRecommendation() {
        this.userID = "";
        this.recommendationID = "";
        this.amisCommun = 0;
    }

    public UserRecommendation(String userID, String recommendationID, int amisCommun) {
        this.userID = userID;
        this.recommendationID = recommendationID;
        this.amisCommun = amisCommun;
    }

    public String getuserID() { return userID; }
    public String getrecommendationID() { return recommendationID; }
    public int getamisCommun() { return amisCommun; }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(userID);
        out.writeUTF(recommendationID);
        out.writeInt(amisCommun);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        userID = in.readUTF();
        recommendationID = in.readUTF();
        amisCommun = in.readInt();
    }

    //Comparaison et trie des recommandations
    @Override
    public int compareTo(UserRecommendation o) {
        int compare = -Integer.compare(this.amisCommun, o.amisCommun);
        if (compare != 0) return compare;
        return this.recommendationID.compareTo(o.recommendationID);
    }

    @Override
    public String toString() {
        return recommendationID + "(" + amisCommun + ")";
    }
}