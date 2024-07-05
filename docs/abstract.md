The past decade has seen a surge of studies using passively generated
location-based service (LBS) data to study people’s human mobility patterns,
answering a wide range of questions relating to the regularity and exploitation
nature of individual mobility patterns, urban vitality, commuting patterns,
disaster recovery, and pandemic spreading. As the potential of using such data
to answer those important questions is increasingly being recognized, we know
surprisingly little about these data. Many questions arise, concerning their
data generation process, stability over time, representativeness, the associated
biases, and effects on the mobility metrics derived. The Mobility Analysis
Workflow (MAW), an open-source python library, is designed to be a tool for
users to analyze key attributes of LBS data and evaluate the effects on the
inferred mobility patterns. We introduce the main functionalities of the MAW
including pre-processing steps that deal with oscillation, processing steps to
infer stay locations and update durations, and analysis steps that calculate key
data characteristics and evaluate the effects on inferred mobility patterns.
Compared to a number of other python libraries that take LBS data to infer
mobility patterns (e.g., scikit mobility and trackintel), the key
differentiating factor that MAW has is its focus on data issues and their
effects on the inferred mobility patterns. The library is available open source
at https://github.com/thinklab/MAWpy.
