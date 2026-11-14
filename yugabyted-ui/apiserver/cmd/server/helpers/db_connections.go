package helpers

import (
    "apiserver/cmd/server/logger"
    "context"
    "net"
    "net/url"
    "strconv"
    "time"

    "github.com/yugabyte/gocql"
    "github.com/jackc/pgx/v5/pgxpool"
)

type PgClientConnectionParams struct {
        User     string
        Password string
        Host     string
        Port     int
        Database string
}

func (h *HelperContainer) CreateGoCqlClient(log logger.Logger) *gocql.ClusterConfig {

    // Get list of tservers
    hostNames := []string{}
    tabletServersFuture := make(chan TabletServersFuture)
    go h.GetTabletServersFuture(HOST, tabletServersFuture)
    tabletServersResponse := <-tabletServersFuture
    if tabletServersResponse.Error != nil {
        log.Warnf("failed to get list of tservers for gocql client setup: %s",
            tabletServersResponse.Error)
        // If we fail to get tservers from GetTabletServersFuture, use HOST
        hostNames = append(hostNames, net.JoinHostPort(HOST, strconv.Itoa(YcqlPort)))
    } else {
        // to get hostnames, get all second level keys and only keep if
        // net.SpliHostPort succeeds.
        for _, obj := range tabletServersResponse.Tablets {
            for hostport := range obj {
                host, _, err := net.SplitHostPort(hostport)
                if err != nil {
                    log.Warnf("failed to split hostport %s: %s", hostport, err.Error())
                } else {
                    hostNames = append(hostNames, net.JoinHostPort(host, strconv.Itoa(YcqlPort)))
                }
            }
        }
    }
    log.Infof("initializing gocql client with initial addresses: %v", hostNames)
    // Initialize gocql client
    cluster := gocql.NewCluster(hostNames...)

    cluster.Authenticator = gocql.PasswordAuthenticator{
        Username: DbYcqlUser,
        Password: DbYcqlPassword,
    }

    if Secure {
            cluster.SslOpts = &gocql.SslOptions{
                    CaPath: SslRootCert,
            }
    }

    // Use the same timeout as the Java driver.
    cluster.Timeout = 12 * time.Second

    // Create the session.
    log.Debugf("Initializing gocql client.")

    return cluster
}

func (h *HelperContainer) CreatePgClient(log logger.Logger,
        connectionParams PgClientConnectionParams) (*pgxpool.Pool, error) {

    // pgx reads a URL connection string with net/url (parseURLSettings in
    // https://github.com/jackc/pgx/blob/v5.10.0/pgconn/config.go), so net/url escapes its parts.
    connURL := url.URL{
            Scheme: "postgres",
            User:   url.UserPassword(connectionParams.User, connectionParams.Password),
            Host:   net.JoinHostPort(connectionParams.Host, strconv.Itoa(connectionParams.Port)),
            Path:   connectionParams.Database,
    }
    if Secure {
            params := url.Values{}
            params.Set("sslmode", SslMode)
            if SslRootCert != "" {
                    params.Set("sslrootcert", SslRootCert)
            }
            connURL.RawQuery = params.Encode()
    }

    log.Debugf("Initializing pgx client.")
    conn, err := pgxpool.New(context.Background(), connURL.String())
    return conn, err
}
