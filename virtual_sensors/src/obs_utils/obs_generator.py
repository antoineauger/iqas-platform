class ObsGenerator(object):

	def __init__(self, config):
		self.config = config
		self.generation_method_obs = self.config['generation_method_obs']  # provided from file or generated
		self.path_obs_file = self.config['path_obs_file']  # location of the raw data file

		self.raw_obs_file = open(self.path_obs_file, 'r')

	def generate_one_observation(self):
		""" Read each line (i.e., observation) of the specified file """
		line = self.raw_obs_file.readline()
		if line == '':
			self.raw_obs_file.close()
			line = None
		return line