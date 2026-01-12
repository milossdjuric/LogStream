package main

import (
	"fmt"
	"net"
	"os"
	"runtime"
)

func main() {
	fmt.Printf("=== LogStream Network Diagnostics ===\n")
	fmt.Printf("Platform: %s/%s\n\n", runtime.GOOS, runtime.GOARCH)

	interfaces, err := net.Interfaces()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Available Network Interfaces:")
	fmt.Println("─────────────────────────────────────")

	var recommended []string

	for _, iface := range interfaces {
		if iface.Flags&net.FlagUp == 0 {
			continue
		}

		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}

		hasIPv4 := false
		var ipv4Addr string

		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}

			if ip == nil || ip.IsLoopback() {
				continue
			}

			if ip.To4() != nil {
				hasIPv4 = true
				ipv4Addr = ip.String()
				break
			}
		}

		if !hasIPv4 {
			continue
		}

		fmt.Printf("\n%s:\n", iface.Name)
		fmt.Printf("  Status:      %s\n", getInterfaceStatus(iface.Flags))
		fmt.Printf("  MAC Address: %s\n", iface.HardwareAddr)
		fmt.Printf("  IPv4:        %s\n", ipv4Addr)

		if isWiFiOrEthernet(iface.Name) {
			fmt.Printf("   Recommended for LogStream\n")
			recommended = append(recommended, ipv4Addr)
		}
	}

	fmt.Println("\n─────────────────────────────────────")
	fmt.Println("\nRecommended Setup Commands:")
	fmt.Println("─────────────────────────────────────")

	if len(recommended) == 0 {
		fmt.Println("No suitable interfaces found!")
		return
	}

	primaryIP := recommended[0]

	if runtime.GOOS == "windows" {
		fmt.Printf("\nMachine 1 (Leader):\n")
		fmt.Printf("  run-local.bat leader %s:8001\n", primaryIP)
		fmt.Printf("\nMachine 2 (Follower):\n")
		fmt.Printf("  run-local.bat follower <MACHINE2_IP>:8002\n")
	} else {
		fmt.Printf("\nMachine 1 (Leader):\n")
		fmt.Printf("  ./run-local.sh leader %s:8001\n", primaryIP)
		fmt.Printf("\nMachine 2 (Follower):\n")
		fmt.Printf("  ./run-local.sh follower <MACHINE2_IP>:8002\n")
	}

	fmt.Println("\n─────────────────────────────────────")
	fmt.Println("\nQuick Test:")
	fmt.Println("─────────────────────────────────────")
	fmt.Printf("From another machine, ping this machine:\n")
	fmt.Printf("  ping %s\n", primaryIP)
}

func getInterfaceStatus(flags net.Flags) string {
	status := []string{}
	if flags&net.FlagUp != 0 {
		status = append(status, "UP")
	}
	if flags&net.FlagBroadcast != 0 {
		status = append(status, "BROADCAST")
	}
	if flags&net.FlagMulticast != 0 {
		status = append(status, "MULTICAST")
	}

	result := ""
	for i, s := range status {
		if i > 0 {
			result += " | "
		}
		result += s
	}
	return result
}

func isWiFiOrEthernet(name string) bool {
	wifiNames := []string{"wlan", "wifi", "en0", "Wi-Fi", "eth"}
	ethNames := []string{"eth", "ens", "enp", "eno", "em", "Ethernet"}

	for _, prefix := range wifiNames {
		if len(name) >= len(prefix) && name[:len(prefix)] == prefix {
			return true
		}
	}

	for _, prefix := range ethNames {
		if len(name) >= len(prefix) && name[:len(prefix)] == prefix {
			return true
		}
	}

	return false
}
