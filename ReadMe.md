# CS 500 Project 2 Report
> Finished by Haoyue Ping, Shangqi Wu

### Algorithm
1. Compute all transitive edges.
2. Find edges that are popular.
3. Form Motifs from popular edges.
4. Find Motifs that are popular.

### Tests
##### 1) Time Cost (executed only once)
|Input File | Supp  |Running Time |
|:--------- |:------|------------:|
|toy        | 0.5   |  2min 22sec |
|hit1_small | 0.2   |  1min  9sec |
|hit1       | 0.1   |  2min 12sec |
|hit1       | 0.2   |  2min 19sec |
|xact       | 0.005 | 33min 39sec |

##### 2) Results
In test of xact, the support threshold is 24.235. This program uses its ceiling number 25 as the final threshold. So Motifs of support 24 are filtered.

All other Motifs matched the output given to us.

### Potential Limitation
This program only supports number of tids less than 2<sup>31</sup>.