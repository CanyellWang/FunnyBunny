package recommender.service;


import recommender.domain.Template;

/**
 * Describe: 规则配置服务
 */
public interface RuleService {
    Template getTemplateByAdId(String adId);

    boolean isExist(String adId);
}
