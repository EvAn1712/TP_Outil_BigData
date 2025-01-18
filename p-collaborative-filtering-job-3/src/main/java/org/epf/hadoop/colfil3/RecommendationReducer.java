package org.epf.hadoop.colfil3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.*;

public class RecommendationReducer extends Reducer<Text, UserRecommendation, Text, Text> {
    private static final int TOP_N = 5;
    private Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<UserRecommendation> values, Context context)
            throws IOException, InterruptedException {
        TreeSet<UserRecommendation> topRecommendations = new TreeSet<>((a, b) -> {
            int compare = -Integer.compare(a.getCommonFriends(), b.getCommonFriends());
            if (compare != 0) return compare;
            return a.getRecommendedId().compareTo(b.getRecommendedId());
        });

        // Collect de toutes les recommandations
        for (UserRecommendation val : values) {
            UserRecommendation copy = new UserRecommendation(
                    val.getUserId(),
                    val.getRecommendedId(),
                    val.getCommonFriends()
            );
            topRecommendations.add(copy);

            if (topRecommendations.size() > TOP_N) {
                topRecommendations.pollLast();
            }
        }
        // Écriture des recommandations
        if (!topRecommendations.isEmpty()) {
            List<String> recommendations = new ArrayList<>();
            for (UserRecommendation rec : topRecommendations) {
                recommendations.add(String.format("%s(%d)",
                        rec.getRecommendedId(), rec.getCommonFriends()));
            }
            result.set(String.join(", ", recommendations));
            context.write(key, result);
        }
    }
}