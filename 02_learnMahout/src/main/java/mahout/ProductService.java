package mahout;

import org.apache.mahout.cf.taste.recommender.Recommender;

/**
 */
public class ProductService {
    private Recommender recommender;

    //

    public void setRecommender(Recommender recommender) {
        this.recommender = recommender;
    }
}
