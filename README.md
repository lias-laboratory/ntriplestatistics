# NTripleStatistics

Cardinality is a measure of the number of occurences of a predicate per subject in a dataset. This project calculates the minimum and maximum cardinality values for predicates using a dataset represented as a .nt format file.

## Algorithm presentation

Four algorithms are provided in this project. Three of them calculate predicate cardinalities based on three different cardinality definitions (*Global*, *Local* and *Characteristic Set*). The last algorithm computes the domains of predicates, which is necessary in order to exploit local cardinalities.

The three cardinality definitions vary by the sets of subjects that are considered for calculating the minimum and maximum number of predicate occurences.

The following tiny dataset will be used as sample to illustrate our three cardinality definitions, as well as the notion of the domain of a prediacte.

```
student1 type Student
student1 is_attending .
student1 is .
student1 is_eating .
student2 type Student
student2 is .
student2 is_attending .
student2 is_attending .
student2 is_eating .
student3 type Former
student3 is_sleeping .
student3 is_attending .
```

### Global

For global cardinalities, all the subjects in the dataset are considered. For example with the following dataset: 

We have the global cardinality values: 

* `card(is_attending) = [1-2]`
* `card(is) = [0-1]`
* `card(is_eating) = [0-1]`
* `card(is_sleeping) = [0-1]`

### Local (or Class)

For class (or local) cardinalities, subjects are split into RDFS classes, and cardinality values are computed for each class. For example, with the dataset from the global example, we have the local cardinality values:

* `card(is_attending, Student) = [1-2]`
* `card(is_attending, Former) = [1-1]`

### Characteristic Set

A Characteristic Set (CS) is the set of predicates of a subject. By grouping subjects that share the same CS, we can again calculate cardinalities on smaller groups. When using predicate cardinality, the minimum cardinality is always 1.

For example, using the dataset from the first example, we have the characteristic sets:

* `CS1{type, is_attending, is, is_eating}`
* `CS2{type, is_sleeping, is_attending}`

And the CS cardinalities:

* `card(is_attending, CS1)=[1-2]`
* `card(is_attending, CS2)=[1-1]`

### Domains

Class cardinalities depend on the RDFS schema of the data. In order to use them, it will be necessary to retrieve the domains of predicates, which are provided in the schema.

Using the dataset from the first example, we have:
* `domain(is)=Student`
* `domain(is_sleeping)=Former`

## Requirements

* Java
* Maven (for compiling)

## How to compile

* From the root of directory, execute the following shell command:

```
$ mvn clean package
```

## How to use

Download the executable file or use the execution file from the compilation step, and make sure the data is available as one or more _.nt_ files. 

The command line to execute the program is : 

```
$ java -jar ntriplestatistics-app.jar [parameters]
```

If no parameters are provided, the following help guide is provided, which lists the expected parameters:

```
NTripleStatistics: statistic tools for NTriple files.

Usage: <main class> [-h] -a=<type> -i=<input> -o=<output> [-s=<separator>] [-t=<typePredicateIdentifier>]
  -a, --algorithm=<type>   The algorithm to use: GLOBAL_CARDINALITIES, LOCAL_CARDINALITIES, CS_CARDINALITIES, DOMAIN.
  -h, --help               Print usage help and exit.
  -i, --input=<input>      The N-Triples files (example: *.nt or 2015-11-02-Abutters.way.sorted.nt).
  -o, --output=<output>    The output directory to save the results.
  -s, --separator=<separator>
                           The separator expression between the subject, predicate and object contents.
  -t, --typeidentifier=<typePredicateIdentifier>
                           The type predicate identifier (default: <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>). Only used for the Local Cardinality algorithm.
```

An example for producing global cardinalities is: 

```
$ java -jar ntriplestatistics-app.jar -a GLOBAL_CARDINALITIES -i ../watdiv.nt -o ../global_cardinalities -s "\t"
```

A text file is generated for each algorithm, containing:
* For GLOBAL_CARDINALITIES: `(predicate,minCard,maxCard)` with one line per predicate
* For LOCAL_CARDINALITIES: `(class,predicate,minCard,maxCard)` with one line per predicate and class
* For CS_CARDINALITIES: `(predicate1,predicate2,predicate3,...,maxCardPredicate1,maxCardPredicate2,...)` with one line per CS
* For DOMAIN: `(predicate,domain)` with one line per predicate


## Software license agreement

* Details the license agreement of NTripleStatistics: [LICENSE](LICENSE)

## Historic Contributors (core developers first followed by alphabetical order)

* [Mickael BARON (core developer)](https://www.lias-lab.fr/members/mickaelbaron/)
* [Louise PARKIN (core developer)](https://www.lias-lab.fr/members/louiseparkin/)
* [Stéphane JEAN](https://www.lias-lab.fr/members/stephanejean/)

## Code analysis

* Lines of code : 875
* Programming language : Java

