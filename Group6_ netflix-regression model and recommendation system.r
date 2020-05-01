
Sys.setlocale("LC_ALL","English")

setwd('F:/5310 SQL')
netflix = read.csv('netflix_titles.csv')

IMDb <- read.table(file='data.tsv')

head(IMDb)

IMDbtitle = read.csv('title_basics.csv')

str(IMDbtitle)

newtable <- merge(IMDb, IMDbtitle, by.x = 'V1',by.y='ï..tconst')

head(newtable)

head(netflix)

finaltable <- merge(netflix, newtable, by.x = 'title',by.y='primaryTitle')

str(finaltable)

head(finaltable)

finaltable$V2 = as.numeric(as.character(finaltable$V2))
str(finaltable)

glmmodel = lm(V2~rating+genres,
             data=finaltable)
summary(glmmodel)

toplist <- finaltable[order(finaltable$V2,decreasing =T),]

toplist[1:10,c("title","type","V2")]

TVshow <- toplist%>%
    subset(type =="TV Show")%>%
    subset(release_year =="2019")

movie <- toplist%>%
    subset(type =="Movie") %>%
    subset(release_year =="2019")

movie[1:10,c("title","release_year","type","V2")]

TVshow[1:10,c("title","release_year","type","V2")]


