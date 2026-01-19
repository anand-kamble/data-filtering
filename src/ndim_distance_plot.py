#!/usr/bin/env python3
"""
Program to plot the average distance between two random points in n-dimensional space
as a function of n (number of dimensions).

This demonstrates how distances behave in high-dimensional spaces.
"""

import numpy as np
import matplotlib.pyplot as plt


def calculate_average_distance(n_dim, n_samples):
    """
    Calculate the average Euclidean distance between two random points
    in n-dimensional unit hypercube [0,1]^n.

    Parameters:
    -----------
    n_dim : int
        Number of dimensions
    n_samples : int
        Number of sample pairs to generate

    Returns:
    --------
    float
        Average distance across all samples
    """
    # Generate random points in [0,1]^n
    # Shape: (n_samples, n_dim)
    points_1 = np.random.rand(n_samples, n_dim)
    points_2 = np.random.rand(n_samples, n_dim)

    # Calculate Euclidean distance for each pair
    # Distance = sqrt(sum((p1 - p2)^2))
    distances = np.sqrt(np.sum((points_1 - points_2)**2, axis=1))

    # Return average distance
    return np.mean(distances)


def plot_distance_vs_dimension(max_dim=50, samples_list=None):
    """
    Plot average distance between two random points as a function of dimension
    for different numbers of samples.

    Parameters:
    -----------
    max_dim : int
        Maximum number of dimensions to test
    samples_list : list of int
        List of sample sizes to test. If None, uses [100, 1000, 10000]
    """
    if samples_list is None:
        samples_list = [100, 1000, 10000]

    dimensions = np.arange(1, max_dim + 1)

    # Set random seed for reproducibility
    np.random.seed(42)

    # Create figure
    plt.figure(figsize=(12, 8))

    # For each sample size, calculate and plot average distances
    for n_samples in samples_list:
        print(f"Computing distances for {n_samples} samples...")
        avg_distances = []

        for n_dim in dimensions:
            avg_dist = calculate_average_distance(n_dim, n_samples)
            avg_distances.append(avg_dist)

            if n_dim % 10 == 0:
                print(f"  Dimension {n_dim}: avg distance = {avg_dist:.4f}")

        # Plot results
        plt.plot(dimensions, avg_distances, marker='o', markersize=3,
                label=f'{n_samples} samples', linewidth=2)

    # Add theoretical curve (for unit hypercube, expected distance ≈ sqrt(n/6))
    theoretical = np.sqrt(dimensions / 6.0)
    plt.plot(dimensions, theoretical, 'k--', linewidth=2,
            label='Theoretical (√(n/6))', alpha=0.7)

    plt.xlabel('Number of Dimensions (n)', fontsize=12)
    plt.ylabel('Average Distance', fontsize=12)
    plt.title('Average Distance Between Two Random Points in n-Dimensional Unit Hypercube',
             fontsize=14, pad=20)
    plt.legend(fontsize=10)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()

    # Save figure
    output_file = 'ndim_distance_plot.png'
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"\nPlot saved to: {output_file}")

    plt.show()


def main():
    """Main function to run the analysis."""
    print("=" * 70)
    print("N-Dimensional Distance Analysis")
    print("=" * 70)
    print("\nThis program calculates the average Euclidean distance between")
    print("two random points in n-dimensional space [0,1]^n")
    print("=" * 70)
    print()

    # Run analysis with different sample sizes
    max_dimensions = 50
    sample_sizes = [100, 1000, 10000]

    plot_distance_vs_dimension(max_dim=max_dimensions, samples_list=sample_sizes)

    print("\nAnalysis complete!")


if __name__ == "__main__":
    main()
