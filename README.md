# Thesis---Scalable-Entity-Resolution-in RDF-data--- performed at University of Bonn
Entity Resolution is the task of disambuigating entities in different datasets that point to the same real world object. 

The naive approach for this task implies O(N^2) complexity. The existing approaches of entity disambiguation in RDF data try to improve efficiency by using a block building strategy, such that entities with in a block are only compared, thus reducing the complexity by O(m^2 * |B|), where m are the entities in a block and we have |B| blocks. After the block building stage, these blocks are preocessed effecitively using a learning or non-learning based approach.
Further, problems exist due to heetrogeneity of data as it arises from a variety of sources with increasing volume. Also, the data is noisy. It has inconsistencies and missing values.

Our thesis work focuses on:-
1. Improving the effeciency and effectiveness of existing approach by introducing scalability.
2. Dealing with the heterogeneity of data and missing values.
3. Learning based approaches require several iterations and effort for labelling sufficient data for training. We want to introduce an approach that can process entities directly in one go.
4. Remove the block building stage completely.

The current thesis work implements two approaches for performing entity resolution task effectively with efficiency and scalability.
We suggest the use of  Local Senstivity Hashing(LSH) for detecting similar entities in semantic web data in the following way.

Approach-1:- Utilise all available knowledge.
It does not work because the data is heterogeneous, we have missing values and inconsistencies.

Approach-2:- Select only 1 or 2 attributes.
Datasets:- DBLP-ACM, DBLP-scholar, Abt-Buy

Approach-3:- LSH subjects in the RDF data -> Compare the predicates of only matching entities by Jaccard similarity threshold for predicates and find the intersecting predicate -> Compare the objects of only intersecting entities in the entity matches found to retrieve the true matches.
Datasets:- Dbpedia medium(Infobox 3.0rc and Infobox 3.4), Dbpedia large(Dbpedia 3.0rc and Infobox 3.4)

The implementation is done on top of SANSA-Stack, using the spark and scala technologies.
We use HDFS for large datasets storage.
