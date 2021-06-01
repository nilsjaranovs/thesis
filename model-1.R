library("tidymodels")
library("doParallel")
library("themis")
library("xgboost")
library(data.table)
library(splitTools)
library(vip)

### Load the model data (depending on iteration)
iteration <- "1_17"

model_dt <- readRDS(paste0("./Data/Modeling Data/dt_",iteration,".RDS"))

id.vars <- c("opportunityid","contactid","accountid")
Y = "QO"
model_dt$QO <- factor(model_dt$QO, levels = c("1","0"))
X.numeric <- names(select_if(model_dt, is.numeric))
X.factors <- names(model_dt)[!(names(model_dt) %in% c(id.vars,X.numeric,Y))]

# Train and test data - with all accounts either only test or train.

# set.seed(1984)
# split <- initial_split(data=model_dt, prop = 0.5, strata = QO)
set.seed(1984)
splitting <- partition(model_dt$accountid, c(analysis=0.7,assessment=0.3),type="grouped",seed=1998)

split <- make_splits(splitting, model_dt)

model_train <- training(split)
model_test <- testing(split)

 # summary(model_test)
 # summary(model_train)

# XGB Fast ----------------------------------------------------------------

set.seed(1984)

cv_folds <- model_train %>% group_vfold_cv(v = 10, group="accountid")

xgb_recipe <- recipe(QO ~ ., data = model_train) %>% 
  step_dummy(!!!syms(X.factors), one_hot=T) %>% 
  step_rm("accountid","contactid","opportunityid")

model_dt[QO==0,.N]/model_dt[QO==1,.N]

xgb_model_tune <- boost_tree(trees = 2453,tree_depth = 6,
                             learn_rate = tune(), stop_iter = 1000) %>%  
  set_mode("classification") %>% 
  set_engine("xgboost", scale_pos_weight = 5.5)

#Set scale_pos_weight to inverse of class distribution (QO=0/QO=1)

xgb_tune_wf <- workflow() %>%
  add_recipe(xgb_recipe) %>%
  add_model(xgb_model_tune)

xgb_tune_wf

class_metrics <- metric_set(accuracy, specificity, sens, 
                            roc_auc)

registerDoParallel()

set.seed(1984)

xgb_grid <- expand.grid(learn_rate=c(0.5))
# 
# xgb_grid <- grid_max_entropy(trees(range = c(0, 10000)),
#                              size = 5)

xgb_tune_res <- tune_grid(
  xgb_tune_wf,
  resamples = cv_folds,
  grid = xgb_grid,
  metrics = class_metrics
) #This one takes ages

 saveRDS(xgb_tune_res, paste0("./Output/model_",iteration,"/model_",iteration,"_tunedwf_temp.RDS"))
# xgb_tune_res <- readRDS(paste0("./Output/model_",iteration,"/model_",iteration,"_tunedwf_temp.RDS"))


xgb_tune_metrics <- xgb_tune_res %>%
  collect_metrics()

xgb_tune_metrics

# saveRDS(xgb_tune_metrics, "./Output/xgb_tuning_metrics3.RDS")

xgb_tune_metrics %>% 
  filter(.metric == "accuracy") %>% 
  ggplot(aes(x = trees, y = mean)) +
  geom_path() +
  labs(y = "Accuracy")

xgb_tune_metrics %>% 
  filter(.metric == "sens") %>% 
  ggplot(aes(x = trees, y = mean)) +
  geom_path() +
  labs(y = "Sensitivity")

xgb_tune_metrics %>% 
  filter(.metric == "spec") %>% 
  ggplot(aes(x = trees, y = mean)) +
  geom_path() +
  labs(y = "Specificityy")

xgb_tune_metrics %>% 
  filter(.metric == "roc_auc") %>% 
  ggplot(aes(x = trees, y = mean)) +
  geom_path() +
  labs(y = "ROC AUC")

# Choose best model

best_sens <- select_by_pct_loss(xgb_tune_res, "sens", limit = 2)

xgb_final_wf <- xgb_tune_wf %>% 
  finalize_workflow(best_sens) %>% 
  fit(model_train)

 xgb_final_wf

xgb_final_fit <- xgb_final_wf %>%
  last_fit(split, metrics = class_metrics)

# Save model results ------------------------------------------------------

saveRDS(xgb_final_wf, paste0("./Output/model_",iteration,"/model_",iteration,".RDS"))
saveRDS(xgb_final_fit, paste0("./Output/model_",iteration,"/model_",iteration,"_finalfit.RDS"))


# Tune Good ---------------------------------------------------------------

set.seed(1984)

cv_folds <- model_train %>% group_vfold_cv(v = 10, group="accountid")

xgb_recipe <- recipe(QO ~ ., data = model_train) %>% 
  step_dummy(!!!syms(X.factors), one_hot=T) %>% 
  step_rm("accountid","contactid","opportunityid")

model_dt[QO==0,.N]/model_dt[QO==1,.N]

xgb_model_tune <- boost_tree(trees = tune(),tree_depth = 6,
                             learn_rate = 0.5, stop_iter = 1000) %>%  
  set_mode("classification") %>% 
  set_engine("xgboost", scale_pos_weight = 5.5)

#Set scale_pos_weight to inverse of class distribution (QO=0/QO=1)

xgb_tune_wf <- workflow() %>%
  add_recipe(xgb_recipe) %>%
  add_model(xgb_model_tune)

xgb_tune_wf

class_metrics <- metric_set(accuracy, specificity, sens, 
                            roc_auc)

registerDoParallel()

set.seed(1984)

#xgb_grid <- expand.grid(learn_rate=c(0.5))
# 
xgb_grid <- grid_max_entropy(trees(range = c(0, 10000)),
                             size = 5)

xgb_tune_res <- tune_grid(
  xgb_tune_wf,
  resamples = cv_folds,
  grid = xgb_grid,
  metrics = class_metrics
) #This one takes ages

saveRDS(xgb_tune_res, paste0("./Output/model_",iteration,"/model_",iteration,"_tunedwf_temp.RDS"))
# xgb_tune_res <- readRDS(paste0("./Output/model_",iteration,"/model_",iteration,"_tunedwf_temp.RDS"))


xgb_tune_metrics <- xgb_tune_res %>%
  collect_metrics()

xgb_tune_metrics

# saveRDS(xgb_tune_metrics, "./Output/xgb_tuning_metrics3.RDS")

xgb_tune_metrics %>% 
  filter(.metric == "accuracy") %>% 
  ggplot(aes(x = trees, y = mean)) +
  geom_path() +
  labs(y = "Accuracy")

xgb_tune_metrics %>% 
  filter(.metric == "sens") %>% 
  ggplot(aes(x = trees, y = mean)) +
  geom_path() +
  labs(y = "Sensitivity")

xgb_tune_metrics %>% 
  filter(.metric == "spec") %>% 
  ggplot(aes(x = trees, y = mean)) +
  geom_path() +
  labs(y = "Specificityy")

xgb_tune_metrics %>% 
  filter(.metric == "roc_auc") %>% 
  ggplot(aes(x = trees, y = mean)) +
  geom_path() +
  labs(y = "ROC AUC")

# Choose best model

best_sens <- select_best(xgb_tune_res, metric="sens")

xgb_final_wf <- xgb_tune_wf %>% 
  finalize_workflow(best_sens) %>% 
  fit(model_train)

xgb_final_wf

xgb_final_fit <- xgb_final_wf %>%
  last_fit(split, metrics = class_metrics)

# Save model results ------------------------------------------------------

saveRDS(xgb_final_wf, paste0("./Output/model_",iteration,"/model_",iteration,"-2.RDS"))
saveRDS(xgb_final_fit, paste0("./Output/model_",iteration,"/model_",iteration,"_finalfit-2.RDS"))

