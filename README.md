# DNA Spark Project
## Initial Setup
1. Download PyCharm.
2. Clone project from GitHub.
3. Open Project in PyCharm.
4. Go to **File > Settings > Project Interpreter** and add PySpark and Jupyter.
5. Install bowtie2.
    * In terminal set up Bioconda path.
    ```
    conda config --add channels defaults
    conda config --add channels conda-forge
    conda config --add channels bioconda
    ```
    * Then install bowtie2.
    ```
    conda install bowtie2
    ```
6. To run click play button on the notebook file. It will ask to connect to Jupyter url, just hit cancel and PyCharm 
will have a popup that you can click on to run the notebook.

## Documentation and important links
[Bowtie 2 documentation](http://bowtie-bio.sourceforge.net/bowtie2/manual.shtml#introduction)

Pre-built Bowtie Indexes can be found [here](http://bowtie-bio.sourceforge.net/bowtie2/index.shtml)

More indexes from illumina's iGenomes [here](https://support.illumina.com/sequencing/sequencing_software/igenome.html)
