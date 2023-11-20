# hartree_test

# Hartree Data Processing Using Pandas and Beam

## Project Description
This project reads data from two CSV datasets, processes the data, and writes the results back to CSV files. There are two scripts, each using a different data processing framework to accomplish the task. The Apache Beam script is named `hartree_pblm_beam.py` and the Pandas script is named `hartree_pblm_pandas.py`.

## Getting Started

### Prerequisites
The project requires Python 3.8 or higher, along with the Apache Beam and Pandas libraries. 

You can install the prerequisites using pip:

```shell
pip install virtualenv
```

## Project Setup

### Clone the Repository

- Clone this repo to your local machine using terminal (macOS, Linux), or Command Prompt (Windows) with the following command:

```shell
git clone <this-repository-url>
```

### Create Virtual Environment

- First navigate to your cloned project:

```shell
cd <path-to-cloned-project>
```

- Next, create a virtual environment, you could name the environment anything you want:

```shell
python -m venv myenv
```
### Activate Virtual Environment
- To activate the virtual environment, navigate to the **Scripts** directory in the virtual environment folder you just created:

  - on macOS and Linux:

  ```shell
  source myenv/bin/activate
  ```

  - on Windows:

  ```shell
  myenv\Scripts\activate
  ```

- Your shell prompt will now show the name of your virtualenv. 

#### Install Required Python Packages

- Install the Python packages that are required for this project, which are listed in `requirements.txt`.

```shell
pip install -r requirements.txt
```

## Run the Project

- In your terminal, ensure you are in the root directory of the project. 
  - To run the program with Apache Beam, type: 

  ```shell
  python hartree_pblm_beam.py
  ```
  - To run the program with the Pandas framework, type:

  ```shell
  python hartree_pblm_pandas.py
  ```

- The output will be a CSV file in the `data` directory.
