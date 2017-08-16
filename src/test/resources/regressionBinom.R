setwd("projects/unicorn/scripts/hail/src/test/resources/")

x <- read.csv("regressionBinom.pheno", h=F, stringsAsFactors = F)
colnames(x) <- c("x1", "x2")

y11 <- c(0, 1, 0, NA, 0, 0, 1, 0)
y12 <- c(2, 1, 2, NA, 2, 2, 1, 2)

dat1 <- cbind(y11, y12, x)
dat1 <- dat1[complete.cases(dat1), ]

m1 <- glm(cbind(y11, y12) ~ x1 + x2, data = dat1, family="binomial")
m1$coefficients
##(Intercept)          x1          x2 
##    -2.108       -1.290       -2.493  

vcov(m1)
##            (Intercept)        x1         x2
##(Intercept)   1.3513670 1.0669574  0.8454704
##              1.0669574 6.4543139  0.2405991
##              0.8454704 0.2405991 11.2835353