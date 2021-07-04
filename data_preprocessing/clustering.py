from kneed import KneeLocator
from matplotlib import pyplot as plt
from sklearn.cluster import KMeans
from data_preprocessing import file_methods


class Clustering:

    """
    This class is used to divide the data into clusters before training.
    """
    def __init__(self, file, logger):
        self.file = file
        self.logger = logger

    def elbow_plot(self, data):
        """
        Method Name: elbow_plot
        Description: This method saves the plot to decide the optimum number of clusters to the file.
        Output: A picture saved to the directory
        On Failure: Raise Exception

        """
        self.logger.log(self.file, 'Entered the elbow_plot method of the KMeansClustering class')
        wcss = []  # initializing an empty list
        try:
            for i in range(1, 11):
                kmeans = KMeans(n_clusters=i, init='k-means++', random_state=42)  # initializing the KMeans object
                kmeans.fit(data)  # fitting the data to the KMeans Algorithm
                wcss.append(kmeans.inertia_)
            plt.plot(range(1, 11), wcss)  # creating the graph between WCSS and the number of clusters
            plt.title('The Elbow Method')
            plt.xlabel('Number of clusters')
            plt.ylabel('WCSS')
            # plt.show()
            plt.savefig('preprocessing_data/K-Means_Elbow.PNG')  # saving the elbow plot locally
            # finding the value of the optimum cluster programmatically
            self.kn = KneeLocator(range(1, 11), wcss, curve='convex', direction='decreasing')
            self.logger.log(self.file, 'The optimum number of clusters is: ' + str(
                self.kn.knee) + ' . Exited the elbow_plot method of the KMeansClustering class')
            return self.kn.knee

        except Exception as e:
            self.logger.log(self.file,
                                   'Exception occured in elbow_plot method of the KMeansClustering class. Exception message:  ' + str(
                                       e))
            self.logger.log(self.file,
                                   'Finding the number of clusters failed. Exited the elbow_plot method of the KMeansClustering class')
            raise Exception()

    def create_clusters(self, data, number_of_clusters):
        """
                                Method Name: create_clusters
                                Description: Create a new dataframe consisting of the cluster information.
                                Output: A datframe with cluster column
                                On Failure: Raise Exception

                                Written By: iNeuron Intelligence
                                Version: 1.0
                                Revisions: None

                        """
        self.logger.log(self.file, 'Entered the create_clusters method of the KMeansClustering class')
        self.data = data
        try:
            self.kmeans = KMeans(n_clusters=number_of_clusters, init='k-means++', random_state=42)
            # self.data = self.data[~self.data.isin([np.nan, np.inf, -np.inf]).any(1)]
            self.y_kmeans = self.kmeans.fit_predict(data)  # divide data into clusters

            self.file_op = file_methods.FileOperation(self.file, self.logger)
            self.save_model = self.file_op.save_model(self.kmeans, 'KMeans')  # saving the KMeans model to directory
            # passing 'Model' as the functions need three parameters

            self.data['Cluster'] = self.y_kmeans  # create a new column in dataset for storing the cluster information
            self.logger.log(self.file, 'succesfully created ' + str(
                self.kn.knee) + 'clusters. Exited the create_clusters method of the KMeansClustering class')
            return self.data
        except Exception as e:
            self.logger.log(self.file,
                                   'Exception occured in create_clusters method of the KMeansClustering class. Exception message:  ' + str(e))
            self.logger.log(self.file,
                                   'Fitting the data to clusters failed. Exited the create_clusters method of the KMeansClustering class')
            raise Exception()






