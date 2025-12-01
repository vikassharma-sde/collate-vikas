# The Ingestion Game

The Ingestion Game consists of:
1. A data file with a varying number of entries containing some data. It is shaped as a CSV where each row has
    an arbitrary number of `key=value` pairs separated by comma.
    ```
    key1=value1,key2=value2
    key3=value3
    key4=value4,key5=value5,key6=value6
    ```
2. Not all `key`s are always important! Each time we play the game, the keys to process can be different. The rest
    are just considered noise. We want to ingest all the important keys.
3. All important keys are always going to be present in the data rows of the input file.
4. Each `key` supports a specific data type. We should only process rows where the values match the expected datatype of each key.
5. Each row consists of an Entity of a given type. The type will be informed as `type=T` in the data. `type` is
    always considered as an important key and we can assume it is a `str`.
6. Entities have hierarchies defined in the games' rules. For example, given the rule `A -> B`, we cannot
    process any Entity of type `B` until we have processed all Entities of type `A`.

Your task is to win the Ingestion Game with the following rules:

- The important keys are `id:int`, `name:str`, `food:str` and `type:str`.
- Entities follow the hierarchy `A -> B -> C`.

The solution must be coded in Python and you can use any public libraries. The solution must be a Python package that we can execute as a CLI that outputs
the processed data to `stdout` in CSV format following the important keys, e.g.,:

```
id,name,food,type
1,levy,veggies,A
2,lima,pizza,A
3,john,fish,B
```

We are going to provide a data file with the keys and hierarchies mentioned above, but your CLI should accept any
important keys and hierarchies as inputs. You can see an example on input data in the provided `input.txt`.

## 🤓 We value in the solution

- Good software design
- Proper documentation
- Compliance to Python standards and modern usages (e.g.: [PEP8](https://peps.python.org/pep-0008/))
- Proper use of data structures
- Ergonomy of the command line interface
- Setup/Launch instructions if required

# Architecture Review

The second exercise is creating a sample architecture for your proposed solution. Instead of considering the package as a CLI, let's suppose you are now developing an "Ingestion Game" SaaS where users can upload their input data, specify the hierarchy and important keys, and expect to get the results back.

For this exercise, prepare one slide explaining the solution design in the cloud of your choice. We value topics such as scalability, reliability, portability, etc.