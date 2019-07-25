% load dataset "accidents"
load accidents
% hwydata is a dataset of type double
x = hwydata(:,14); %Population of states
y = hwydata(:,4); %Accidents per state
format long
b1 = x\y  % the slope of simple linear regression

yCalc1 = b1*x;
scatter(x,y)
hold on
plot(x,yCalc1)
xlabel('Population of state')
ylabel('Fatal traffic accidents per state')
title('Linear Regression Relation Between Accidents & Population')
grid on

length(x)
ones(length(x),1)
X = [ones(length(x),1) x];
b = X\y

yCalc2 = X*b;  % matrix multiplication
plot(x,yCalc2,'--')  % using dashed line
legend('Data','Slope','Slope & Intercept','Location','best');

% compute the R-squared
Rsq1 = 1 - sum((y - yCalc1).^2)/sum((y - mean(y)).^2)
Rsq2 = 1 - sum((y - yCalc2).^2)/sum((y - mean(y)).^2)

load count.dat
x = count(:,1);
y = count(:,2);

% fit with linear relation
p = polyfit(x,y,1)  % p: slope and intercept
yfit = polyval(p,x);  % prediction
% equivalently
yfit =  p(1) * x + p(2);

yresid = y - yfit;
SSresid = sum(yresid.^2);
var(y) % variance of y
SStotal = (length(y)-1) * var(y);


% cubic fit
load count.dat
x = count(:,1);
y = count(:,2);
p = polyfit(x,y,3)
yfit = polyval(p,x);
% equivalently
yfit =  p(1) * x.^3 + p(2) * x.^2 + p(3) * x + p(4);

yresid = y - yfit;
SSresid = sum(yresid.^2);
SStotal = (length(y)-1) * var(y);

rsq = 1 - SSresid/SStotal
rsq_adj = 1 - SSresid/SStotal * (length(y)-1)/(length(y)-length(p))

scatter(x,y);
hold on
plot(x,yfit);  % ?











