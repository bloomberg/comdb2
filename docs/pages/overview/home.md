---
title: Comdb2
keywords: homepage
sidebar: mydoc_sidebar
permalink: overview_home.html
---

## What is Comdb2?

Comdb2 is a relational database built in-house at Bloomberg L.P. over the last 14 years 
or so.  It started with a modest goal of replacing an older home-grown system to allow 
databases to stay in sync easier.  SQL was added early in its development, and it quickly 
started replacing other relational databases in addition to its original goal.  Comdb2 
today holds a good chunk of Bloomberg's data, and is continually developed by a dedicated team.

## Why did you build your own database?

We had several goals in mind.  One was being wire-format compatible with an older
internal product to allow developers to migrate applications easier.  Another was to
have a database that could be updated anywhere and stay in sync everywhere.  The
first goal could only be satisfied in-house.  The second was either in its infancy
for open source products, or available at high cost from commercial sources.
Neither option looked appealing at the time.  We did not start from scratch.
Comdb2 builds on lots of open source software.  A good review of the system could be found
in our paper presented at VLDB 2016, found at <http://www.vldb.org/pvldb/vol9/p1377-scotti.pdf>

## Where to start?

Start with the 'Overview' topics on the left sidebar. [Your first database](example_db.html) is 
a useful read.  It should give you a taste of what using the database looks like.  If you're 
inspired to experiment with it, [Installing Comdb2](install.html) will get you started 
on installing and running the software.  [The Comdb2 transaction model](transaction_model.html) 
explains some inner workings and what to expect from various transaction modes.

When you're ready to start writing some code, check out the 'Programming for Comdb2' 
topics. [SQL language](sql.html) details the SQL dialect we use. [Data types](datatypes.html) 
lists the supported data types. APIs for some common languages are available along with other 
miscellaneous development topics.

The remaining sections are dedicated to supporting databases. Details of configuring and 
operating Comdb2 are to be found there.
