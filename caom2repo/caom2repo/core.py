#!/usr/bin/env python2.7
# # -*- coding: utf-8 -*-
# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2016.                            (c) 2016.
#  Government of Canada                 Gouvernement du Canada
#  National Research Council            Conseil national de recherches
#  Ottawa, Canada, K1A 0R6              Ottawa, Canada, K1A 0R6
#  All rights reserved                  Tous droits réservés
#
#  NRC disclaims any warranties,        Le CNRC dénie toute garantie
#  expressed, implied, or               énoncée, implicite ou légale,
#  statutory, of any kind with          de quelque nature que ce
#  respect to the software,             soit, concernant le logiciel,
#  including without limitation         y compris sans restriction
#  any warranty of merchantability      toute garantie de valeur
#  or fitness for a particular          marchande ou de pertinence
#  purpose. NRC shall not be            pour un usage particulier.
#  liable in any event for any          Le CNRC ne pourra en aucun cas
#  damages, whether direct or           être tenu responsable de tout
#  indirect, special or general,        dommage, direct ou indirect,
#  consequential or incidental,         particulier ou général,
#  arising from the use of the          accessoire ou fortuit, résultant
#  software.  Neither the name          de l'utilisation du logiciel. Ni
#  of the National Research             le nom du Conseil National de
#  Council of Canada nor the            Recherches du Canada ni les noms
#  names of its contributors may        de ses  participants ne peuvent
#  be used to endorse or promote        être utilisés pour approuver ou
#  products derived from this           promouvoir les produits dérivés
#  software without specific prior      de ce logiciel sans autorisation
#  written permission.                  préalable et particulière
#                                       par écrit.
#
#  This file is part of the             Ce fichier fait partie du projet
#  OpenCADC project.                    OpenCADC.
#
#  OpenCADC is free software:           OpenCADC est un logiciel libre ;
#  you can redistribute it and/or       vous pouvez le redistribuer ou le
#  modify it under the terms of         modifier suivant les termes de
#  the GNU Affero General Public        la “GNU Affero General Public
#  License as published by the          License” telle que publiée
#  Free Software Foundation,            par la Free Software Foundation
#  either version 3 of the              : soit la version 3 de cette
#  License, or (at your option)         licence, soit (à votre gré)
#  any later version.                   toute version ultérieure.
#
#  OpenCADC is distributed in the       OpenCADC est distribué
#  hope that it will be useful,         dans l’espoir qu’il vous
#  but WITHOUT ANY WARRANTY;            sera utile, mais SANS AUCUNE
#  without even the implied             GARANTIE : sans même la garantie
#  warranty of MERCHANTABILITY          implicite de COMMERCIALISABILITÉ
#  or FITNESS FOR A PARTICULAR          ni d’ADÉQUATION À UN OBJECTIF
#  PURPOSE.  See the GNU Affero         PARTICULIER. Consultez la Licence
#  General Public License for           Générale Publique GNU Affero
#  more details.                        pour plus de détails.
#
#  You should have received             Vous devriez avoir reçu une
#  a copy of the GNU Affero             copie de la Licence Générale
#  General Public License along         Publique GNU Affero avec
#  with OpenCADC.  If not, see          OpenCADC ; si ce n’est
#  <http://www.gnu.org/licenses/>.      pas le cas, consultez :
#                                       <http://www.gnu.org/licenses/>.
#
#  $Revision: 4 $
#
# ***********************************************************************
#
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import argparse
import imp
import logging
import multiprocessing
from multiprocessing import Lock, Pool
import os
import os.path
import sys
from datetime import datetime

from cadcutils import net
from cadcutils import util
from caom2.obs_reader_writer import ObservationReader, ObservationWriter
from caom2.version import version as caom2_version
from six import BytesIO

# from . import version as caom2repo_version
from caom2repo import version

__all__ = ['CAOM2RepoClient']

BATCH_SIZE = int(10000)

CAOM2REPO_OBS_CAPABILITY_ID =\
    'vos://cadc.nrc.ca~vospace/CADC/std/CAOM2Repository#obs-1.1'

# resource ID for info
DEFAULT_RESOURCE_ID = 'ivo://cadc.nrc.ca/caom2repo'
APP_NAME = 'caom2repo'


class CAOM2RepoClient(object):
    """Class to do CRUD + visitor actions on a CAOM2 collection repo."""

    logger = logging.getLogger('CAOM2RepoClient')

    def __init__(self, subject, resource_id=DEFAULT_RESOURCE_ID, host=None):
        """
        Instance of a CAOM2RepoClient
        :param subject: the subject performing the action
        :type cadcutils.auth.Subject
        :param server: Host server for the caom2repo service
        """
        agent = '{}/{}'.format(APP_NAME, version.version)
        self.host = host
        self._subject = subject
        self._repo_client = net.BaseWsClient(resource_id, subject, agent,
                                             retry=True, host=host)

        agent = "caom2-repo-client/{} caom2/{}".format(version.version,
                                                       caom2_version)

        self._repo_client = net.BaseWsClient(resource_id, subject,
                                             agent, retry=True, host=self.host)

    # shortcuts for the CRUD operations
    def create(self, observation):
        """
        Creates an observation in the repo.
        :param observation: Observation to create
        :return: Created observation
        """
        self.put_observation(observation)

    def read(self, collection, observation_id):
        """
        Read an observation from the repo
        :param collection: Name of the collection
        :param observation_id: Observation identifier
        :return: Observation
        """
        self.get_observation(collection, observation_id)

    def update(self, observation):
        """
        Update an observation in the repo
        :param observation: Observation to update
        :return: Updated observation
        """
        self.post_observation(observation)

    def delete(self, collection, observation_id):
        """
        Delete an observation from the repo
        :param collection: Name of the collection
        :param observation_id: Observation identifier
        """
        self.delete_observation(collection, observation_id)

    def visit(self, plugin, collection, start=None, end=None, obs_file=None,
              nthreads=None, halt_on_error=False):
        """
        Main processing function that iterates through the observations of
        the collection and updates them according to the algorithm
        of the plugin function
        :param plugin: path to python file that contains the algorithm to be
                applied to visited observations
        :param collection: name of the CAOM2 collection
        :param start: optional earliest date-time of the targeted observation
               set
        :param end: optional latest date-time of the targeted observation set
        :param halt_on_error if True halts the execution on the first exception
               raised by the plugin update function otherwise logs the error
               and continues
        :return: tuple (list of visited observations, list of updated
               observation, list of skipped observations, list of failure
               observations)
        """
        if not os.path.isfile(plugin):
            raise Exception('Cannot find plugin file ' + plugin)
        assert collection is not None
        if start is not None:
            assert type(start) is datetime
        if end is not None:
            assert type(end) is datetime

        self._load_plugin_class(plugin)

        # this is updated by _get_observations with the timestamp of last
        # observation in the batch
        self._start = start
        visited = []
        failed = []
        updated = []
        skipped = []
        observations = []
        if obs_file is not None:
            # get observation IDs from file, no batching
            observations = self._get_obs_from_file(obs_file, start, end, halt_on_error)
        else:
            # get observation IDs from caomrepo
            observations = self._get_observations(collection, self._start, end)

        while len(observations) > 0:
            if nthreads is None:
                for observationID in observations:
                    v, u, s, f= self._process_observation_id(collection, observationID, halt_on_error)
                    if v:
                        visited.append(v)
                    if u:
                        updated.append(u)
                    if s:
                        skipped.append(s)
                    if f:
                        failed.append(f)

            else:
                mutex = Lock()
                p = Pool(nthreads)
                results = [p.apply_async(
                    self.multiprocess_observation_id,
                    [collection, observationID, self.get_observation, self.post_observation, halt_on_error,
                     self.logger, self.plugin, mutex])
                    for observationID in observations]
                for r in results:
                    v, u, s, f= r.get()
                    if v:
                        visited.append(v)
                    if u:
                        updated.append(u)
                    if s:
                        skipped.append(s)
                    if f:
                        failed.append(f)

            if obs_file is None and len(observations) == BATCH_SIZE:
                # get observation IDs from caomrepo
                observations = self._get_observations(collection, self._start,
                                                      end)
            else:
                # the last batch was smaller so it must have been the last
                break
        return visited, updated, skipped, failed

    def _process_observation_id(self, collection, observationID, halt_on_error):
        visited = []
        failed = []
        updated = []
        skipped = []
        self.logger.info('Process observation: ' + observationID)
        observation = self.get_observation(collection, observationID)
        try:
            if self.plugin.update(observation=observation,
                             subject=self._subject) is False:
                self.logger.info(
                    'SKIP {}'.format(observation.observation_id))
                skipped.append(observation.observation_id)
            else:
                self.post_observation(observation)
                self.logger.debug(
                    'UPDATED {}'.format(observation.observation_id))
                updated.append(observation.observation_id)
        except TypeError as e:
            if "unexpected keyword argument" in str(e):
                raise RuntimeError(
                    "{} - To fix the problem, please add the **kwargs "
                    "argument to the list of arguments for the update"
                    " method of your plugin.".format(str(e)))
        except Exception as e:
            failed.append(observation.observation_id)
            self.logger.error('FAILED {} - Reason: {}'.
                         format(observation.observation_id, e))
            if halt_on_error:
                raise e

        visited.append(observation.observation_id)

        return visited, updated, skipped, failed

    def multiprocess_observation_id(self, collection, observationID, get_observation, post_observation, halt_on_error,
                                    logger, plugin, mutex):
        visited = []
        failed = []
        updated = []
        skipped = []
        mutex.acquire()
        logger.info('Process observation: ' + observationID)
        mutex.release
        observation = get_observation(collection, observationID)
        try:
            if plugin.update(observation=observation,
                                  subject=self._subject) is False:
                mutex.acquire()
                logger.info(
                    'SKIP {}'.format(observation.observation_id))
                mutex.release
                skipped.append(observation.observation_id)
            else:
                post_observation(observation)
                mutex.acquire()
                logger.debug(
                    'UPDATED {}'.format(observation.observation_id))
                mutex.release
                updated.append(observation.observation_id)
        except TypeError as e:
            if "unexpected keyword argument" in str(e):
                raise RuntimeError(
                    "{} - To fix the problem, please add the **kwargs "
                    "argument to the list of arguments for the update"
                    " method of your plugin.".format(str(e)))
        except Exception as e:
            failed.append(observation.observation_id)
            mutex.acquire()
            logger.error('FAILED {} - Reason: {}'.
                              format(observation.observation_id, e))
            mutex.release
            if halt_on_error:
                raise e
        except KeyboardInterrupt as e:
            # user pressed Control-C or Delete
            mutex.acquire()
            logger.error('FAILED {} - Reason: {}'.
                              format(observation.observation_id, e))
            mutex.release
            raise e
            sys.exit(-2)

        visited.append(observation.observation_id)

        return visited, updated, skipped, failed

    def _get_obs_from_file(self, obs_file, start, end, halt_on_error):
        start_datetime = util.utils.str2ivoa(start)
        end_datetime = util.utils.str2ivoa(end)
        obs = []
        failed = []
        with open(obs_file) as fp:
            for l in fp:
                line = l.rstrip('\n').strip()
                if len(line) > 0:
                    if ' ' in line:
                        # we have at least two tokens in line
                        obs_id, last_modified_date = line.split(' ', 1)
                        try:
                            last_mod_datetime = util.utils.str2ivoa(last_modified_date)
                            if start_datetime is not None:
                                if start_datetime<= last_mod_datetime:
                                    # last_modified_date is same or later than start date
                                    if self._matches_end_date(obs_id, end_datetime, last_mod_datetime):
                                        obs.append(obs_id)
                            elif self._matches_end_date(obs_id, end_datetime, last_mod_datetime):
                                obs.append(obs_id)
                        except Exception as e:
                            failed.append(obs_id)
                            self.logger.error('FAILED {} - Reason: {}'.
                                              format(obs_id, e))
                            if halt_on_error:
                                raise e
                    else:
                        # only one token in line, line should contain observationID only
                        obs.append(line)

        return obs

    def _matches_end_date(self, obs_id, end_datetime, last_mod_datetime):
        matches_end_date = False
        if end_datetime is not None:
            if last_mod_datetime <= end_datetime:
                # and last_modified_date is earlier than end date
                matches_end_date = True
        else:
            # but not end date
            matches_end_date = True

        return matches_end_date

    def _get_observations(self, collection, start=None, end=None, obs_file=None):
        """
        Returns a list of observations from the collection
        :param collection: name of the collection
        :param start: earliest observation
        :param end: latest observation
        :return: list of observation ids
        """
        assert collection is not None
        observations = []
        params = {'MAXREC': BATCH_SIZE}
        if start is not None:
            params['START'] = util.utils.date2ivoa(start)
        if end is not None:
            params['END'] = util.utils.date2ivoa(end)

        response = self._repo_client.get(
            (CAOM2REPO_OBS_CAPABILITY_ID, collection),
            params=params)
        last_datetime = None
        for line in response.text.splitlines():
            columns = line.split('\t')
            if len(columns) >= 3:
                obs = columns[1]
                last_datetime = columns[2]
                observations.append(obs)
            else:
                self.logger.warn('Incomplete listing line: {}'.format(line))
        if last_datetime is not None:
            self._start = util.utils.str2ivoa(last_datetime)
        return observations

    def _load_plugin_class(self, filepath):
        """
        Loads the plugin method and sets the self.plugin to refer to it.
        :param filepath: path to the file containing the python function
        """
        expected_class = 'ObservationUpdater'

        mod_name, file_ext = os.path.splitext(os.path.split(filepath)[-1])

        if file_ext.lower() == '.pyc':
            py_mod = imp.load_compiled(mod_name, filepath)
        else:
            py_mod = imp.load_source(mod_name, filepath)

        if hasattr(py_mod, expected_class):
            self.plugin = getattr(py_mod, expected_class)()
        else:
            raise Exception(
                'Cannot find ObservationUpdater class in pluging file ' +
                filepath)

        if not hasattr(self.plugin, 'update'):
            raise Exception(
                'Cannot find update method in plugin class ' + filepath)

    def get_observation(self, collection, observation_id):
        """
        Get an observation from the CAOM2 repo
        :param collection: name of the collection
        :param observation_id: the ID of the observation
        :return: the caom2.observation.Observation object
        """
        assert collection is not None
        assert observation_id is not None
        path = '/{}/{}'.format(collection, observation_id)
        logging.debug('GET '.format(path))

        response = self._repo_client.get((CAOM2REPO_OBS_CAPABILITY_ID, path))
        obs_reader = ObservationReader()
        content = response.content
        if len(content) == 0:
            logging.error(response.status_code)
            response.close()
            raise Exception('Got empty response for resource: {}'.format(path))
        return obs_reader.read(BytesIO(content))

    def post_observation(self, observation):
        """
        Updates an observation in the CAOM2 repo
        :param observation: observation to update
        :return: updated observation
        """
        assert observation.collection is not None
        assert observation.observation_id is not None
        path = '/{}/{}'.format(observation.collection,
                               observation.observation_id)
        logging.debug('POST {}'.format(path))

        ibuffer = BytesIO()
        ObservationWriter().write(observation, ibuffer)
        obs_xml = ibuffer.getvalue()
        headers = {'Content-Type': 'application/xml'}
        self._repo_client.post(
            (CAOM2REPO_OBS_CAPABILITY_ID, path), headers=headers, data=obs_xml)
        logging.debug('Successfully updated Observation\n')

    def put_observation(self, observation):
        """
        Add an observation to the CAOM2 repo
        :param observation: observation to add to the CAOM2 repo
        :return: Added observation
        """
        assert observation.collection is not None
        assert observation.observation_id is not None
        path = '/{}/{}'.format(observation.collection,
                               observation.observation_id)
        logging.debug('PUT {}'.format(path))

        ibuffer = BytesIO()
        ObservationWriter().write(observation, ibuffer)
        obs_xml = ibuffer.getvalue()
        headers = {'Content-Type': 'application/xml'}
        self._repo_client.put(
            (CAOM2REPO_OBS_CAPABILITY_ID, path), headers=headers, data=obs_xml)
        logging.debug('Successfully put Observation\n')

    def delete_observation(self, collection, observation_id):
        """
        Delete an observation from the CAOM2 repo
        :param collection: Name of the collection
        :param observation_id: ID of the observation
        """
        assert observation_id is not None
        path = '/{}/{}'.format(collection, observation_id)
        logging.debug('DELETE {}'.format(path))
        self._repo_client.delete(
            (CAOM2REPO_OBS_CAPABILITY_ID, path))
        logging.info('Successfully deleted Observation {}\n')


def str2date(s):
    """
    Takes a date formatted string and returns a datetime.

    """
    date_format = '%Y-%m-%dT%H:%M:%S'
    if s is None:
        return None
    return datetime.strptime(s, date_format)


def main_app():
    parser = util.get_base_parser(version=version.version,
                                  default_resource_id=DEFAULT_RESOURCE_ID)

    parser.description = (
        'Client for a CAOM2 repo. In addition to CRUD (Create, Read, Update '
        'and Delete) operations it also implements a visitor operation that '
        'allows for updating multiple observations in a collection')

    parser.add_argument("-s", "--server", help='URL of the CAOM2 repo server')

    parser.formatter_class = argparse.RawTextHelpFormatter

    subparsers = parser.add_subparsers(dest='cmd')
    create_parser = subparsers.add_parser(
        'create', description='Create a new observation',
        help='Create a new observation')
    create_parser.add_argument('observation',
                               help='XML file containing the observation',
                               type=argparse.FileType('r'))

    read_parser = subparsers.add_parser(
        'read', description='Read an existing observation',
        help='Read an existing observation')
    read_parser.add_argument('--output', '-o', help='destination file',
                             required=False)
    read_parser.add_argument('collection',
                             help='collection name in CAOM2 repo')
    read_parser.add_argument('observationID', help='observation identifier')

    update_parser = subparsers.add_parser(
        'update', description='Update an existing observation',
        help='Update an existing observation')
    update_parser.add_argument('observation',
                               help='XML file containing the observation',
                               type=argparse.FileType('r'))

    delete_parser = subparsers.add_parser(
        'delete', description='Delete an existing observation',
        help='Delete an existing observation')
    delete_parser.add_argument('collection',
                               help='collection name in CAOM2 repo')
    delete_parser.add_argument('observationID', help='observation identifier')

    # Note: RawTextHelpFormatter allows for the use of newline in epilog
    visit_parser = subparsers.add_parser(
        'visit', formatter_class=argparse.RawTextHelpFormatter,
        description='Visit observations in a collection',
        help='Visit observations in a collection')
    visit_parser.add_argument('--plugin', required=True,
                              type=argparse.FileType('r'),
                              help='plugin class to update each observation')
    visit_parser.add_argument(
        '--start', type=str2date, help=('earliest observation to visit '
                                        '(UTC IVOA format: YYYY-mm-ddTH:M:S)'))
    visit_parser.add_argument(
        '--end', type=str2date,
        help='latest observation to visit (UTC IVOA format: YYYY-mm-ddTH:M:S)')
    visit_parser.add_argument('--obs_file', help='file containing observations to be visited',
                              type=argparse.FileType('r'))
    visit_parser.add_argument(
        '--threads', type=int,
        help='number of threads the visitor will spawn when getting observations, range is 2 to 10')
    visit_parser.add_argument(
        '--halt-on-error', action='store_true',
        help='stop visitor on first update exception raised by plugin')
    visit_parser.add_argument('collection',
                              help='data collection in CAOM2 repo')

    visit_parser.epilog = \
        """
        Minimum plugin file format:
        ----
           from caom2 import Observation

           class ObservationUpdater:

            def update(self, observation, **kwargs):
                assert isinstance(observation, Observation), (
                    'observation {} is not an Observation'.format(observation))
                # custom code to update the observation
                # other arguments passed by the calling code to the update
                # method:
                #   kwargs['subject'] - user authentication that caom2repo was
                #                       invoked with
        ----
        """
    args = parser.parse_args()
    if len(sys.argv) < 2:
        parser.print_usage(file=sys.stderr)
        sys.stderr.write("{}: error: too few arguments\n".format(APP_NAME))
        sys.exit(-1)
    if args.verbose:
        logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    elif args.debug:
        logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)
    else:
        logging.basicConfig(level=logging.WARN, stream=sys.stdout)

    subject = net.Subject.from_cmd_line_args(args)
    server = None
    if args.server:
        server = args.server

    client = CAOM2RepoClient(subject, args.resource_id, host=server)
    if args.cmd == 'visit':
        print("Visit")
        logging.debug(
            "Call visitor with plugin={}, start={}, end={}, collection={}, obs_file={}, threads={}".
            format(args.plugin.name, args.start, args.end,
                   args.collection, args.obs_file, args.threads))
        if args.threads is not None:
            if args.threads < 2:
                parser.print_usage(file=sys.stderr)
                sys.stderr.write("{}: error: too few threads specified for visitor\n".format(APP_NAME))
                sys.exit(-1)
                #raise ValueError(
                #    "{}: error: too few threads specified for visitor\n".format(APP_NAME))
            elif args.threads > 10:
                parser.print_usage(file=sys.stderr)
                sys.stderr.write("{}: error: too many threads specified for visitor\n".format(APP_NAME))
                sys.exit(-1)
                #raise ValueError(
                #    "{}: error: too many threads specified for visitor\n".format(APP_NAME))
        (visited, updated, skipped, failed) = \
            client.visit(args.plugin.name, args.collection, start=args.start,
                         end=args.end, obs_file=args.obs_file, threads=args.threads,
                         halt_on_error=args.halt_on_error)
        logging.info(
            'Visitor stats: visited/updated/skipped/errors: {}/{}/{}/{}'.
            format(len(visited), len(updated), len(skipped), len(failed)))

    elif args.cmd == 'create':
        logging.info("Create")
        obs_reader = ObservationReader()
        client.put_observation(obs_reader.read(args.observation))
    elif args.cmd == 'read':
        logging.info("Read")
        observation = client.get_observation(args.collection,
                                             args.observationID)
        observation_writer = ObservationWriter()
        if args.output:
            observation_writer.write(observation, args.output)
        else:
            observation_writer.write(observation, sys.stdout)
    elif args.cmd == 'update':
        logging.info("Update")
        obs_reader = ObservationReader()
        # TODO not sure if need to read in string first
        client.post_observation(obs_reader.read(args.observation))
    else:
        logging.info("Delete")
        client.delete_observation(collection=args.collection,
                                  observation_id=args.observationID)

    logging.info("DONE")


if __name__ == '__main__':
    main_app()
