Traffic Modeler
=====================

Tool for transport modeling for Apache Spark. The aim of the project is to implement two step (Origin-Destination matrix estimation and Traffic assignment) of the four-step transport model. All algorithms are implemented in Apache Spark environment, therefore it is possible to create a large models.

Traffic Assignment
------------------
Framework provides following methods for the traffic assignment:

* All-or-nothing (AON)
* User equilibrium (TAP) - example code for this methods are in class com.kolovsky.example.TwoRoadAssignment
    * Path based algorithm (Jayakrishnan 1994)
    * B algorithm (Dial 2006) - limited parallelization

(Jayakrishnan 1994) Jayakrishnan, R., et al. "A faster path-based algorithm for traffic assignment." University of California Transportation Center (1994).

(Dial 2006) Dial, Robert B. "A path-based user-equilibrium traffic assignment algorithm that obviates path storage and enumeration." Transportation Research Part B: Methodological 40.10 (2006): 917-936.
