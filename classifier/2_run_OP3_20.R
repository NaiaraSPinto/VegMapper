library(arm)
library(EBglmnet)

#USER INPUTS
#####------------------------------------------------------------------------------------------------------
data = read.csv("/Users/nspinto/Documents/forest/1_palms/OP3/UCAYALI/calval/ucayali_rs.csv")

#treshold for logistic model; 
# when probability is larger than this threshold, we say that oil palm is present
threshold = 0.5

#no priors for Costa Rica
use_prior = FALSE
#prior_mean = c(0.06491638, -26.63132182, 0.05590800, -29.64091635)
#prior_scale = c(0.02048902, 7.49385721, 0.01658031, 8.75762742)
#prior_mean_int = 1.99274793
#prior_scale_int = 7.33476429

#lower and upper bounds for posterior credible intervals
lp = 0.025
up = 0.975

#columns of the predictor variables to be used in this model
index = c(4,5,6,7) 
#####------------------------------------------------------------------------------------------------------

# Impute missing values by variable means
for (i in which(sapply(data, is.numeric))) {
  for (j in which(is.na(data[, i]))) {
    data[j, i] <- mean(data[data[, "my_class"] == data[j, "my_class"], i],  na.rm = TRUE)
  }
}


# true_label: 1 for oil_palm and 0 for non oil_palm
true_label = 1*(data$my_class == 'oil_palm')  
# transform interested variables into a matrix which would be used
x = as.matrix(data[, index])


# Build model by incorporating those variables
formula = as.formula(paste("true_label ~ ", paste(names(data[, index]), collapse="+"),sep = ""))
use_data = as.data.frame(cbind(x, true_label))

# To specify prior
# If noninformative prior, use prior.mean=apply(x, 2, mean), prior.scale=Inf, prior.df=Inf
# If having a prior, set prior.mean=c(....), prior.scale=c(.....)
# length of prior mean and prior scale should be equal to the number of predictors
if(! use_prior){
	model = bayesglm(formula, data=use_data,family=binomial(link='logit'), prior.mean=apply(x, 2, mean) ,prior.scale=Inf, scale=FALSE)
}
if(use_prior){
	model = bayesglm(formula, data=use_data, family=binomial(link='logit'), 
                    prior.mean=prior_mean,
                    prior.scale=prior_scale,
                    prior.mean.for.intercept=prior_mean_int,
                    prior.scale.for.intercept=prior_scale_int,
		     scale = FALSE)	
}

# oil_palm prediction
class_prediction = 1*(model$fitted.values >= threshold)

# construct confusion matrix bayesian_conf_matrix
bayesian_conf_matrix = matrix(0,2,2)
bayesian_conf_matrix[1,1] = sum(class_prediction + true_label == 0)
bayesian_conf_matrix[2,2] = sum(class_prediction + true_label == 2)
bayesian_conf_matrix[1,2] = sum((class_prediction == 0) & (true_label == 1)) 
bayesian_conf_matrix[2,1] = sum((class_prediction == 1) & (true_label == 0))
rownames(bayesian_conf_matrix) = c("Predicted non-oil-palm", "Predicted oil-palm")
colnames(bayesian_conf_matrix) = c("Actual non-oil-palm", "Actual oil-palm")
bayesian_conf_matrix

# calculate prediction accuracy, posterior CI
accu_bayes = sum(class_prediction == true_label) / nrow(data)
print(accu_bayes)


# approach posterior distributions of coefficients
# specify number of draws 
num_draw = 2000
post_dist = sim(model, n.sims=num_draw)
coef_matrix = coef(post_dist)

# calculate posterior credible intervals for coefficients
posterior_ci_coef = matrix(NA, ncol(x)+1, 2)
for (i in 1:(ncol(x)+1)){
  posterior_ci_coef[i, ] = unname(quantile(coef_matrix[, i], probs=c(lp, up), na.rm=TRUE))
}

# calculate posterior credible intervals for every data point
posterior_ci_data = matrix(NA, nrow(x), 2)
for(i in 1:nrow(x)){
  temp = as.numeric()
  for(j in 1:num_draw){
    temp[j] = 1 / (1 + exp(-coef_matrix[j, 1] - sum(coef_matrix[j, 2:(length(index)+1)] * x[i, ])))
  }
  posterior_ci_data[i, ] = unname(quantile(temp, probs=c(lp, up), na.rm=TRUE))
}


# build posterior objects for next run
posterior_mean = model$coefficients
posterior_scale = apply(coef(post_dist), 2, sd)

#print the posteriors
posterior_mean


