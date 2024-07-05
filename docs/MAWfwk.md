# MAW Framework

## MAW Input Files

- variety of inputs that MAW can take (csv, dataframes and others);
- input fields (required and optional)
- describe the use of standardized names for computing efficiency

## MAW Output Files

- talk about output files: columns
- describe the use of standardized names

## MAW core data processing functionalities

### DP: Data partition (optional)

partition datasets into different sets with different uncertainty radius (if
available)

### OSC: Addressing Oscillation

review this paper
(https://www.sciencedirect.com/science/article/pii/S0968090X17303637?via%3Dihub#s0035)
and the algorithm itself. test.

### TIME4OSC: Identifying time window for oscillation removal

review this paper
https://www.sciencedirect.com/science/article/pii/S0968090X17303637?via%3Dihub#f0015,
section 4.3.2. Create plot such as Figure 10

### TRACESEG: Identifying stays from low-variance (GPS) data

trace segmentation algorithm. review this paper
https://www-sciencedirect-com.offcampus.lib.washington.edu/science/article/pii/S0968090X18316085?via%3Dihub#fn4,
section 4.2.1 as well as Appendix A.1. and the algorithm itself. Test algorithm.

### INCREMENTAL: Identifying stays from high-variance (cellular) data

incremental clustering algorithm. review the above paper, section 4.2.2. And
test algorithm.

### STAYINT: Integrate stays

refer the above paper section 4.3 and the algorithms itself

### STAYCAL: Update stays

update stays and duration.

### STAY_SP_THRESHOLD: Identifying suitable spatial thresholds for stay identification (km)

create plots that shows number of stays per user per day. see Figure 7 in this
paper:
https://www.sciencedirect.com/science/article/pii/S0968090X17303637?via%3Dihub#f0015

### STAY_TIME_THRESHOLD: Identifying suitable temporal thresholds for stay identification (min)

create plots that shows number of stays per user per day. see Figure 7 in this
paper:
https://www.sciencedirect.com/science/article/pii/S0968090X17303637?via%3Dihub#f0015

## MAW core analysis functionalities

### S

### S_USER: Single user analysis

show a user's raw trajectory on a map with stays identified along with duration
information. See MAW paper figure 1, with the underlying map.

it shall take a single user's data only. If a user inputs multiple users' data,
an error shall be returned.

### A_USER: Aggregate user analysis

shows many users' stays on a map with stays being aggregated as POIs

### HOME: Identifying home locations

identifying home locations require multiple days' data (a minimum of five days).
Below is the rule: the home location is identified as the tract containing the
stay with the most frequent visits during the night time (22:00 pm to 6:00 am
the next day), with the condition that it has to be visited at least X times for
a time period. See homeloc_threshold.png for the criteria. After 21 days, it
requires at least once a week.
